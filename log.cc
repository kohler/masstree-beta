/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012 President and Fellows of Harvard College
 * Copyright (c) 2012 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, subject to the conditions
 * listed in the Masstree LICENSE file. These conditions include: you must
 * preserve this copyright notice, and you cannot mention the copyright
 * holders in advertising related to the Software without their permission.
 * The Software is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This
 * notice is a summary of the Masstree LICENSE file; the license in that file is
 * legally binding.
 */
#include "log.hh"
#include "kvthread.hh"
#include "kvrow.hh"
#include "file.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

kvepoch_t global_log_epoch;
kvepoch_t global_wake_epoch;
struct timeval log_epoch_interval;
static struct timeval log_epoch_time;

kvepoch_t rec_ckp_min_epoch;
kvepoch_t rec_ckp_max_epoch;
logreplay::info_type *rec_log_infos;
kvepoch_t rec_replay_min_epoch;
kvepoch_t rec_replay_max_epoch;
kvepoch_t rec_replay_min_quiescent_last_epoch;

static void *logger(void *);

void
log::initialize(int i, const char *logdir)
{
    f_.lock_ = 0;

    struct stat sb;
    int r = stat(logdir, &sb);
    if (r < 0 && errno == ENOENT) {
	r = mkdir(logdir, 0777);
	if (r < 0) {
	    fprintf(stderr, "%s: %s\n", logdir, strerror(errno));
	    mandatory_assert(0);
	}
    }

    r = snprintf(filename_, sizeof(filename_), "%s/kvd-log-%d", logdir, i);
    mandatory_assert(size_t(r) < sizeof(filename_));

    len_ = 20 * 1024 * 1024;
    pos_ = 0;
    buf_ = (char *) malloc(len_);
    mandatory_assert(buf_);
    log_epoch_ = 0;
    quiescent_epoch_ = 0;
    wake_epoch_ = 0;
    flushed_epoch_ = 0;

    ti_ = threadinfo::make(threadinfo::TI_LOG, i);
    r = pthread_create(&ti_->ti_threadid, 0, ::logger, this);
    mandatory_assert(r == 0);
}

void
log::add_log_pending(threadinfo *ti)
{
    if (f_.ti_tail_)
	f_.ti_tail_->log_pending_list_ = ti;
    else
	f_.ti_head_ = ti;
    f_.ti_tail_ = ti;
}

void
log::remove_log_pending(threadinfo *ti)
{
    assert(f_.ti_head_ == ti);
    if (!(f_.ti_head_ = ti->log_pending_list_))
	f_.ti_tail_ = 0;
    ti->log_pending_list_ = 0;
}

// one logger thread per logs[].
void *
logger(void *xarg)
{
    struct log *l = static_cast<struct log *>(xarg);
    l->logger();
    return 0;
}

static void
check_epoch()
{
    struct timeval tv;
    gettimeofday(&tv, 0);
    if (timercmp(&tv, &log_epoch_time, >)) {
	log_epoch_time = tv;
	timeradd(&log_epoch_time, &log_epoch_interval, &log_epoch_time);
	global_log_epoch = global_log_epoch.next_nonzero(); // 0 isn't valid
    }
}

void
log::logger()
{
    ti_->enter();
    {
	logreplay replayer(filename_);
	replayer.replay(ti_->ti_index, ti_);
    }

    int fd = open(filename_, O_WRONLY|O_APPEND|O_CREAT, 0666);
    mandatory_assert(fd >= 0);
    char *x_buf = (char *) malloc(len_);
    mandatory_assert(x_buf);

    while (1) {
	uint32_t nb = 0;
	acquire();
	kvepoch_t ge = global_log_epoch, we = global_wake_epoch;
	if (wake_epoch_ != we) {
	    wake_epoch_ = we;
	    quiescent_epoch_ = 0;
	}
	// If the writing threads appear quiescent, and aren't about to write
	// to the log (f_.ti_head_ != 0), then write a quiescence
	// notification.
	if (!recovering && pos_ == 0 && !quiescent_epoch_
	    && ge != log_epoch_ && ge != we && !f_.ti_head_) {
	    quiescent_epoch_ = log_epoch_ = ge;
	    char *p = buf_;
	    p += logrec_epoch::store(p, logcmd_epoch, log_epoch_);
            if (log_epoch_ == wake_epoch_)
                p += logrec_base::store(p, logcmd_wake);
	    p += logrec_base::store(p, logcmd_quiesce);
	    pos_ = p - buf_;
	}
	if (!recovering && pos_ > 0) {
	    uint32_t x_pos = pos_;
	    std::swap(buf_, x_buf);
	    pos_ = 0;
	    kvepoch_t x_epoch = log_epoch_;
	    release();
	    ssize_t r = write(fd, x_buf, x_pos);
	    mandatory_assert(r == ssize_t(x_pos));
	    fsync(fd);
	    flushed_epoch_ = x_epoch;
	    // printf("log %d %d\n", ti_->ti_index, x_pos);
	    nb = x_pos;
	} else
	    release();
	if (nb < len_ / 4)
	    napms(200);
	if (ti_->ti_index == 0)
	    check_epoch();
    }
}


// replay

logreplay::logreplay(const String &filename)
    : filename_(filename), errno_(0), buf_()
{
    int fd = open(filename_.c_str(), O_RDONLY);
    if (fd == -1) {
    fail:
	errno_ = errno;
	buf_ = 0;
	if (fd != -1)
	    (void) close(fd);
	return;
    }

    struct stat sb;
    int r = fstat(fd, &sb);
    if (r == -1)
	goto fail;

    size_ = sb.st_size;
    if (size_ != 0) {
	// XXX what if filename_ is too big to mmap in its entirety?
	// XXX should support mmaping/writing in pieces
	buf_ = (char *) ::mmap(0, size_, PROT_READ, MAP_FILE | MAP_PRIVATE,
			       fd, 0);
	if (buf_ == MAP_FAILED)
	    goto fail;
    }

    (void) close(fd);
}

logreplay::~logreplay()
{
    unmap();
}

int
logreplay::unmap()
{
    int r = 0;
    if (buf_) {
	r = munmap(buf_, size_);
	buf_ = 0;
    }
    return r;
}


struct logrecord {
    uint32_t command;
    str key;
    str val;
    kvtimestamp_t ts;
    kvtimestamp_t prev_ts;
    kvepoch_t epoch;

    const char *extract(const char *buf, const char *end);
};

const char *
logrecord::extract(const char *buf, const char *end)
{
    const logrec_base *lr = reinterpret_cast<const logrec_base *>(buf);
    if (unlikely(size_t(end - buf) < sizeof(*lr)
		 || lr->size_ < sizeof(*lr)
		 || size_t(end - buf) < lr->size_
		 || lr->command_ == logcmd_none)) {
    fail:
	command = logcmd_none;
	return end;
    }

    command = lr->command_;
    if (command == logcmd_put || command == logcmd_put1
	|| command == logcmd_remove) {
	const logrec_kv *lk = reinterpret_cast<const logrec_kv *>(buf);
	if (unlikely(lk->size_ < sizeof(*lk)
		     || lk->keylen_ > MaxKeyLen
		     || sizeof(*lk) + lk->keylen_ > lk->size_))
	    goto fail;
	ts = lk->ts_;
	key.assign(lk->buf_, lk->keylen_);
	val.assign(lk->buf_ + lk->keylen_, lk->size_ - sizeof(*lk) - lk->keylen_);
    } else if (command == logcmd_modify) {
	const logrec_kvdelta *lk = reinterpret_cast<const logrec_kvdelta *>(buf);
	if (unlikely(lk->keylen_ > MaxKeyLen
		     || sizeof(*lk) + lk->keylen_ > lk->size_))
	    goto fail;
	ts = lk->ts_;
	prev_ts = lk->prev_ts_;
	key.assign(lk->buf_, lk->keylen_);
	val.assign(lk->buf_ + lk->keylen_, lk->size_ - sizeof(*lk) - lk->keylen_);
    } else if (command == logcmd_epoch) {
	const logrec_epoch *lre = reinterpret_cast<const logrec_epoch *>(buf);
	if (unlikely(lre->size_ < logrec_epoch::size()))
	    goto fail;
	epoch = lre->epoch_;
    }

    return buf + lr->size_;
}


logreplay::info_type
logreplay::info() const
{
    info_type x;
    x.first_epoch = x.last_epoch = x.wake_epoch = x.min_post_quiescent_wake_epoch = 0;
    x.quiescent = true;

    const char *buf = buf_, *end = buf_ + size_;
    off_t nr = 0;
    bool log_corrupt = false;
    while (buf + sizeof(logrec_base) <= end) {
	const logrec_base *lr = reinterpret_cast<const logrec_base *>(buf);
	if (unlikely(lr->size_ < sizeof(logrec_base))) {
	    log_corrupt = true;
	    break;
	} else if (unlikely(buf + lr->size_ > end))
	    break;
	x.quiescent = lr->command_ == logcmd_quiesce;
	if (lr->command_ == logcmd_epoch) {
	    const logrec_epoch *lre =
		reinterpret_cast<const logrec_epoch *>(buf);
	    if (unlikely(lre->size_ < sizeof(*lre))) {
		log_corrupt = true;
		break;
	    }
	    if (!x.first_epoch)
		x.first_epoch = lre->epoch_;
	    x.last_epoch = lre->epoch_;
	    if (x.wake_epoch && x.wake_epoch > x.last_epoch) // wrap-around
		x.wake_epoch = 0;
	} else if (lr->command_ == logcmd_wake)
	    x.wake_epoch = x.last_epoch;
#if !NDEBUG
	else if (lr->command_ != logcmd_put
		 && lr->command_ != logcmd_put1
		 && lr->command_ != logcmd_modify
		 && lr->command_ != logcmd_remove
		 && lr->command_ != logcmd_quiesce) {
	    log_corrupt = true;
	    break;
	}
#endif
	buf += lr->size_;
	++nr;
    }

    fprintf(stderr, "replay %s: %" PRIdOFF_T " records, first %" PRIu64 ", last %" PRIu64 ", wake %" PRIu64 "%s%s @%zu\n",
	    filename_.c_str(), nr, x.first_epoch.value(),
	    x.last_epoch.value(), x.wake_epoch.value(),
	    x.quiescent ? ", quiescent" : "",
	    log_corrupt ? ", CORRUPT" : "", buf - buf_);
    return x;
}

kvepoch_t
logreplay::min_post_quiescent_wake_epoch(kvepoch_t quiescent_epoch) const
{
    kvepoch_t e = 0;
    const char *buf = buf_, *end = buf_ + size_;
    bool log_corrupt = false;
    while (buf + sizeof(logrec_base) <= end) {
	const logrec_base *lr = reinterpret_cast<const logrec_base *>(buf);
	if (unlikely(lr->size_ < sizeof(logrec_base))) {
	    log_corrupt = true;
	    break;
	} else if (unlikely(buf + lr->size_ > end))
	    break;
	if (lr->command_ == logcmd_epoch) {
	    const logrec_epoch *lre =
		reinterpret_cast<const logrec_epoch *>(buf);
	    if (unlikely(lre->size_ < sizeof(*lre))) {
		log_corrupt = true;
		break;
	    }
	    e = lre->epoch_;
	} else if (lr->command_ == logcmd_wake
		   && e
		   && e >= quiescent_epoch)
	    return e;
	buf += lr->size_;
    }
    (void) log_corrupt;
    return 0;
}

uint64_t
logreplay::replayandclean1(query<row_type> &q,
			   kvepoch_t min_epoch, kvepoch_t max_epoch,
			   threadinfo *ti)
{
    uint64_t nr = 0;
    const char *pos = buf_, *end = buf_ + size_;
    const char *repbegin = 0, *repend = 0;
    logrecord lr;

    // XXX
    while (pos < end) {
	const char *nextpos = lr.extract(pos, end);
	if (lr.command == logcmd_none) {
	    fprintf(stderr, "replay %s: %" PRIu64 " entries replayed, CORRUPT @%zu\n",
		    filename_.c_str(), nr, pos - buf_);
	    break;
	}
	if (lr.command == logcmd_epoch) {
	    if ((min_epoch && lr.epoch < min_epoch)
		|| (!min_epoch && !repbegin))
		repbegin = pos;
	    if (lr.epoch >= max_epoch) {
		mandatory_assert(repbegin);
		repend = nextpos;
		break;
	    }
	}
	if (!lr.epoch || (min_epoch && lr.epoch < min_epoch)) {
	    pos = nextpos;
	    if (repbegin)
		repend = nextpos;
	    continue;
	}
	// replay only part of log after checkpoint
	// could replay everything, the if() here tests
	// correctness of checkpoint scheme.
	assert(repbegin);
	repend = nextpos;
	if (lr.key.len) { // skip empty entry
	    if (lr.command == logcmd_put) {
		q.begin_replay_put(lr.key, lr.val, lr.ts);
		tree->put(q, ti);
	    } else if (lr.command == logcmd_put1) {
		q.begin_replay_put1(lr.key, lr.val, lr.ts);
		tree->put(q, ti);
	    } else if (lr.command == logcmd_modify) {
		q.begin_replay_modify(lr.key, lr.val, lr.ts, lr.prev_ts);
		tree->put(q, ti);
	    } else if (lr.command == logcmd_remove) {
		q.begin_replay_remove(lr.key, lr.ts, ti);
		tree->put(q, ti);
	    }
	    ++nr;
	    if (nr % 100000 == 0)
		fprintf(stderr,
			"replay %s: %" PRIu64 " entries replayed\n",
			filename_.c_str(), nr);
	}
	pos = nextpos;
    }

    // rewrite portion of log
    if (!repbegin)
	repbegin = repend = buf_;
    else if (!repend) {
	fprintf(stderr, "replay %s: surprise repend\n", filename_.c_str());
	repend = pos;
    }

    char tmplog[256];
    int r = snprintf(tmplog, sizeof(tmplog), "%s.tmp", filename_.c_str());
    mandatory_assert(r >= 0 && size_t(r) < sizeof(tmplog));

    printf("replay %s: truncate from %" PRIdOFF_T " to %" PRIdSIZE_T " [%" PRIdSIZE_T ",%" PRIdSIZE_T ")\n",
	   filename_.c_str(), size_, repend - repbegin,
	   repbegin - buf_, repend - buf_);

    bool need_copy = repbegin != buf_;
    int fd;
    if (!need_copy)
	fd = replay_truncate(repend - repbegin);
    else
	fd = replay_copy(tmplog, repbegin, repend);

    r = fsync(fd);
    mandatory_assert(r == 0);
    r = close(fd);
    mandatory_assert(r == 0);

    // replace old log with rewritten log
    if (unmap() != 0)
	abort();

    if (need_copy) {
	r = rename(tmplog, filename_.c_str());
	if (r != 0) {
	    fprintf(stderr, "replay %s: %s\n", filename_.c_str(), strerror(errno));
	    abort();
	}
    }

    return nr;
}

int
logreplay::replay_truncate(size_t len)
{
    int fd = open(filename_.c_str(), O_RDWR);
    if (fd < 0) {
	fprintf(stderr, "replay %s: %s\n", filename_.c_str(), strerror(errno));
	abort();
    }

    struct stat sb;
    int r = fstat(fd, &sb);
    if (r != 0) {
	fprintf(stderr, "replay %s: %s\n", filename_.c_str(), strerror(errno));
	abort();
    } else if (sb.st_size < off_t(len)) {
	fprintf(stderr, "replay %s: bad length %" PRIdOFF_T "\n", filename_.c_str(), sb.st_size);
	abort();
    }

    r = ftruncate(fd, len);
    if (r != 0) {
	fprintf(stderr, "replay %s: truncate: %s\n", filename_.c_str(), strerror(errno));
	abort();
    }

    off_t off = lseek(fd, len, SEEK_SET);
    if (off == (off_t) -1) {
	fprintf(stderr, "replay %s: seek: %s\n", filename_.c_str(), strerror(errno));
	abort();
    }

    return fd;
}

int
logreplay::replay_copy(const char *tmpname, const char *first, const char *last)
{
    int fd = creat(tmpname, 0666);
    if (fd < 0) {
	fprintf(stderr, "replay %s: create: %s\n", tmpname, strerror(errno));
	abort();
    }

    ssize_t w = safe_write(fd, first, last - first);
    mandatory_assert(w >= 0 && w == last - first);

    return fd;
}

void
logreplay::replay(int which, threadinfo *ti)
{
    waituntilphase(REC_LOG_TS);
    query<row_type> q;
    // find the maximum timestamp of entries in the log
    if (buf_) {
	info_type x = info();
	pthread_mutex_lock(&rec_mu);
	rec_log_infos[which] = x;
	pthread_mutex_unlock(&rec_mu);
    }
    inactive();

    waituntilphase(REC_LOG_ANALYZE_WAKE);
    if (buf_) {
        if (rec_replay_min_quiescent_last_epoch
            && rec_replay_min_quiescent_last_epoch <= rec_log_infos[which].wake_epoch)
            rec_log_infos[which].min_post_quiescent_wake_epoch =
                min_post_quiescent_wake_epoch(rec_replay_min_quiescent_last_epoch);
    }
    inactive();

    waituntilphase(REC_LOG_REPLAY);
    if (buf_) {
	ti->rcu_start();
	uint64_t nr = replayandclean1(q, rec_replay_min_epoch, rec_replay_max_epoch, ti);
	ti->rcu_stop();
	printf("recovered %" PRIu64 " records from %s\n", nr, filename_.c_str());
    }
    inactive();
}
