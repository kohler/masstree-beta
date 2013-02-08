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
#ifndef KVDB_LOG_HH
#define KVDB_LOG_HH 1
#include "kvdconfig.hh"
#include "str.hh"
#include "string.hh"
#include "circular_int.hh"
#include <pthread.h>
class kvtable;
class threadinfo;
template <typename R> struct query;

// in-memory log.
// more than one, to reduce contention on the lock.
struct log {
  private:
    struct front {
	uint32_t lock_;
	threadinfo *ti_head_;
	threadinfo *ti_tail_;
    };
    union {
	front f_;
	char cache_line_[CacheLineSize];
    };

  public:
    char *buf_;
    uint32_t pos_;
    uint32_t len_;

    kvepoch_t log_epoch_;	// epoch written to log (non-quiescent)
    kvepoch_t quiescent_epoch_;	// epoch we went quiescent
    kvepoch_t wake_epoch_;	// epoch for which we recorded a wake command
    kvepoch_t flushed_epoch_;	// epoch fsync()ed to disk
    // We have logged all writes up to, but not including, flushed_epoch_.
    // Log is quiesced to disk if quiescent_epoch_ != 0
    // and quiescent_epoch_ == flushed_epoch_.
    // When a log wakes up from quiescence, it sets global_wake_epoch;
    // other threads must record a logcmd_wake in their logs.
    // Invariant: log_epoch_ != quiescent_epoch_ (unless both are 0).

    char filename_[128];

    void initialize(int i, const char *logdir);
    void logger();

    void acquire() {
	test_and_set_acquire(&f_.lock_);
    }
    void release() {
	test_and_set_release(&f_.lock_);
    }

    // logging
    void add_log_pending(threadinfo *ti);
    void remove_log_pending(threadinfo *ti);
    bool log_is_ready(threadinfo *ti) const {
	return f_.ti_head_ == ti;
    }

  private:
    threadinfo *ti_;

    friend class threadinfo;
};

class logreplay {
  public:
    logreplay(const String &filename);
    ~logreplay();
    int unmap();

    struct info_type {
	kvepoch_t first_epoch;
	kvepoch_t last_epoch;
	kvepoch_t wake_epoch;
	kvepoch_t min_post_quiescent_wake_epoch;
	bool quiescent;
    };
    info_type info() const;
    kvepoch_t min_post_quiescent_wake_epoch(kvepoch_t quiescent_epoch) const;

    void replay(int i, threadinfo *ti);

  private:
    String filename_;
    int errno_;
    off_t size_;
    char *buf_;

    uint64_t replayandclean1(query<row_type> &q,
			     kvepoch_t min_epoch, kvepoch_t max_epoch,
			     threadinfo *ti);
    int replay_truncate(size_t len);
    int replay_copy(const char *tmpname, const char *first, const char *last);
};

extern kvepoch_t global_log_epoch;
extern kvepoch_t global_wake_epoch;
extern struct timeval log_epoch_interval;


enum logcommand {
    logcmd_none = 0,
    logcmd_put = 0x5455506B,		// "kPUT" in little endian
    logcmd_put1 = 0x3155506B,		// "kPU1"
    logcmd_modify = 0x444F4D6B,		// "kMOD"
    logcmd_remove = 0x4D45526B,		// "kREM"
    logcmd_epoch = 0x4F50456B,		// "kEPO"
    logcmd_quiesce = 0x4955516B,	// "kQUI"
    logcmd_wake = 0x4B41576B		// "kWAK"
};

struct logrec_base {
    uint32_t command_;
    uint32_t size_;

    static size_t size() {
	return sizeof(logrec_base);
    }
    static size_t store(char *buf, uint32_t command) {
	// XXX check alignment on some architectures
	logrec_base *lr = reinterpret_cast<logrec_base *>(buf);
	lr->command_ = command;
	lr->size_ = sizeof(*lr);
	return sizeof(*lr);
    }
    static bool check(const char *buf) {
	const logrec_base *lr = reinterpret_cast<const logrec_base *>(buf);
	return lr->size_ >= sizeof(*lr);
    }
    static uint32_t command(const char *buf) {
	const logrec_base *lr = reinterpret_cast<const logrec_base *>(buf);
	return lr->command_;
    }
};

struct logrec_epoch {
    uint32_t command_;
    uint32_t size_;
    kvepoch_t epoch_;

    static size_t size() {
	return sizeof(logrec_epoch);
    }
    static size_t store(char *buf, uint32_t command, kvepoch_t epoch) {
	// XXX check alignment on some architectures
	logrec_epoch *lr = reinterpret_cast<logrec_epoch *>(buf);
	lr->command_ = command;
	lr->size_ = sizeof(*lr);
	lr->epoch_ = epoch;
	return sizeof(*lr);
    }
    static bool check(const char *buf) {
	const logrec_epoch *lr = reinterpret_cast<const logrec_epoch *>(buf);
	return lr->size_ >= sizeof(*lr);
    }
};

struct logrec_kv {
    uint32_t command_;
    uint32_t size_;
    kvtimestamp_t ts_;
    uint32_t keylen_;
    char buf_[0];

    static size_t size(uint32_t keylen, uint32_t vallen) {
	return sizeof(logrec_kv) + keylen + vallen;
    }
    static size_t store(char *buf, uint32_t command,
			str key, str val,
			kvtimestamp_t ts) {
	// XXX check alignment on some architectures
	logrec_kv *lr = reinterpret_cast<logrec_kv *>(buf);
	lr->command_ = command;
	lr->size_ = sizeof(*lr) + key.len + val.len;
	lr->ts_ = ts;
	lr->keylen_ = key.len;
	memcpy(lr->buf_, key.s, key.len);
	memcpy(lr->buf_ + key.len, val.s, val.len);
	return sizeof(*lr) + key.len + val.len;
    }
    static bool check(const char *buf) {
	const logrec_kv *lr = reinterpret_cast<const logrec_kv *>(buf);
	return lr->size_ >= sizeof(*lr)
	    && lr->size_ >= sizeof(*lr) + lr->keylen_;
    }
};

struct logrec_kvdelta {
    uint32_t command_;
    uint32_t size_;
    kvtimestamp_t ts_;
    kvtimestamp_t prev_ts_;
    uint32_t keylen_;
    char buf_[0];

    static size_t size(uint32_t keylen, uint32_t vallen) {
	return sizeof(logrec_kvdelta) + keylen + vallen;
    }
    static size_t store(char *buf, uint32_t command,
			str key, str val,
			kvtimestamp_t prev_ts, kvtimestamp_t ts) {
	// XXX check alignment on some architectures
	logrec_kvdelta *lr = reinterpret_cast<logrec_kvdelta *>(buf);
	lr->command_ = command;
	lr->size_ = sizeof(*lr) + key.len + val.len;
	lr->ts_ = ts;
	lr->prev_ts_ = prev_ts;
	lr->keylen_ = key.len;
	memcpy(lr->buf_, key.s, key.len);
	memcpy(lr->buf_ + key.len, val.s, val.len);
	return sizeof(*lr) + key.len + val.len;
    }
    static bool check(const char *buf) {
	const logrec_kvdelta *lr = reinterpret_cast<const logrec_kvdelta *>(buf);
	return lr->size_ >= sizeof(*lr)
	    && lr->size_ >= sizeof(*lr) + lr->keylen_;
    }
};


enum { REC_NONE, REC_CKP, REC_LOG_TS, REC_LOG_ANALYZE_WAKE,
       REC_LOG_REPLAY, REC_DONE };
extern void recphase(int nactive, int state);
extern void waituntilphase(int phase);
extern void inactive();
extern pthread_mutex_t rec_mu;
extern logreplay::info_type *rec_log_infos;
extern kvepoch_t rec_ckp_min_epoch;
extern kvepoch_t rec_ckp_max_epoch;
extern kvepoch_t rec_replay_min_epoch;
extern kvepoch_t rec_replay_max_epoch;
extern kvepoch_t rec_replay_min_quiescent_last_epoch;

extern kvtable *tree;

#endif
