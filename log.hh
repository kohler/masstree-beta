/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2013 President and Fellows of Harvard College
 * Copyright (c) 2012-2013 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, subject to the conditions
 * listed in the Masstree LICENSE file. These conditions include: you must
 * preserve this copyright notice, and you cannot mention the copyright
 * holders in advertising related to the Software without their permission.
 * The Software is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This
 * notice is a summary of the Masstree LICENSE file; the license in that file
 * is legally binding.
 */
#ifndef KVDB_LOG_HH
#define KVDB_LOG_HH
#include "kvthread.hh"
#include "string.hh"
#include "kvproto.hh"
#include "serial_changeset.hh"
#include <pthread.h>
template <typename R> class replay_query;
class logset;

// in-memory log.
// more than one, to reduce contention on the lock.
class loginfo {
  public:
    void initialize(const String& logfile);
    void logger();

    inline void acquire();
    inline void release();

    inline kvepoch_t flushed_epoch() const;
    inline bool quiescent() const;

    // logging
    struct query_times {
        kvepoch_t epoch;
        kvtimestamp_t ts;
        kvtimestamp_t prev_ts;
    };
    // NB may block!
    void record(int command, const query_times& qt, Str key, Str value);

  private:
    struct waitlist {
        waitlist* next;
    };
    struct front {
	uint32_t lock_;
	waitlist* waiting_;
        String::rep_type filename_;
        logset* logset_;
    };
    struct logset_info {
        int32_t size_;
        int allocation_offset_;
    };

    front f_;
    char padding1_[CacheLineSize - sizeof(front)];

    kvepoch_t log_epoch_;       // epoch written to log (non-quiescent)
    kvepoch_t quiescent_epoch_; // epoch we went quiescent
    kvepoch_t wake_epoch_;      // epoch for which we recorded a wake command
    kvepoch_t flushed_epoch_;   // epoch fsync()ed to disk

    union {
        struct {
            char *buf_;
            uint32_t pos_;
            uint32_t len_;

            // We have logged all writes up to, but not including,
            // flushed_epoch_.
            // Log is quiesced to disk if quiescent_epoch_ != 0
            // and quiescent_epoch_ == flushed_epoch_.
            // When a log wakes up from quiescence, it sets global_wake_epoch;
            // other threads must record a logcmd_wake in their logs.
            // Invariant: log_epoch_ != quiescent_epoch_ (unless both are 0).

            threadinfo *ti_;
            int logindex_;
        };
        struct {
            char cache_line_2_[CacheLineSize - 4 * sizeof(kvepoch_t) - sizeof(logset_info)];
            logset_info lsi_;
        };
    };

    loginfo(logset* ls, int logindex);
    ~loginfo();

    friend class logset;
};

class logset {
  public:
    static logset* make(int size);
    static void free(logset* ls);

    inline int size() const;
    inline loginfo& log(int i);
    inline const loginfo& log(int i) const;

  private:
    loginfo li_[0];
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

    uint64_t replayandclean1(replay_query<row_type> &q,
			     kvepoch_t min_epoch, kvepoch_t max_epoch,
			     threadinfo *ti);
    int replay_truncate(size_t len);
    int replay_copy(const char *tmpname, const char *first, const char *last);
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


inline void loginfo::acquire() {
    test_and_set_acquire(&f_.lock_);
}

inline void loginfo::release() {
    test_and_set_release(&f_.lock_);
}

inline kvepoch_t loginfo::flushed_epoch() const {
    return flushed_epoch_;
}

inline bool loginfo::quiescent() const {
    return quiescent_epoch_ && quiescent_epoch_ == flushed_epoch_;
}

inline int logset::size() const {
    return li_[-1].lsi_.size_;
}

inline loginfo& logset::log(int i) {
    assert(unsigned(i) < unsigned(size()));
    return li_[i];
}

inline const loginfo& logset::log(int i) const {
    assert(unsigned(i) < unsigned(size()));
    return li_[i];
}


template <typename R>
struct row_delta_marker : public row_marker {
    kvtimestamp_t prev_ts_;
    R *prev_;
    char s_[0];
};

template <typename R>
inline bool row_is_delta_marker(const R* row) {
    if (row_is_marker(row)) {
	const row_marker* m =
	    reinterpret_cast<const row_marker *>(row->col(0).s);
	return m->marker_type_ == m->mt_delta;
    } else
	return false;
}

template <typename R>
inline row_delta_marker<R>* row_get_delta_marker(const R* row, bool force = false) {
    (void) force;
    assert(force || row_is_delta_marker(row));
    return reinterpret_cast<row_delta_marker<R>*>
	(const_cast<char*>(row->col(0).s));
}

template <typename R>
class replay_query {
  public:
    enum {
	QT_Replay_Put = 1,
        QT_Replay_Put1 = 2,
	QT_Replay_Remove = 3,
	QT_Replay_Modify = 4
    };

    void begin_replay_put(Str key, Str req, kvtimestamp_t ts);
    void begin_replay_put1(Str key, Str value, kvtimestamp_t ts);
    void begin_replay_modify(Str key, Str req, kvtimestamp_t ts,
			     kvtimestamp_t prev_ts);
    void begin_replay_remove(Str key, kvtimestamp_t ts, threadinfo* ti);

    int query_type() const {
	return qt_;
    }
    const loginfo::query_times& query_times() const {
        return qtimes_;
    }

    void apply(R*& value, bool has_value, threadinfo* ti);

  private:
    loginfo::query_times qtimes_;
  public:
    Str key_;   // startkey for scan; key for others
  private:
    int qt_;    // query type
    Str val_;			// value for Get1 and CkpPut
};

template <typename R>
void replay_query<R>::begin_replay_put(Str key, Str req, kvtimestamp_t ts) {
    qt_ = QT_Replay_Put;
    key_ = key;
    val_ = req;
    qtimes_.ts = ts;
}

template <typename R>
void replay_query<R>::begin_replay_put1(Str key, Str value, kvtimestamp_t ts) {
    qt_ = QT_Replay_Put1;
    key_ = key;
    val_ = value;
    qtimes_.ts = ts;
}

template <typename R>
void replay_query<R>::begin_replay_modify(Str key, Str req,
					  kvtimestamp_t ts, kvtimestamp_t prev_ts) {
    // XXX We assume that sizeof(row_delta_marker<R>) memory exists before
    // 'req's string data. We don't modify this memory but it must be
    // readable. This is OK for conventional log replay, but that's an ugly
    // interface
    qt_ = QT_Replay_Modify;
    key_ = key;
    val_ = req;
    qtimes_.ts = ts;
    qtimes_.prev_ts = prev_ts;
}

template <typename R>
void replay_query<R>::begin_replay_remove(Str key, kvtimestamp_t ts, threadinfo* ti) {
    qt_ = QT_Replay_Remove;
    key_ = key;
    qtimes_.ts = ts | 1;        // marker timestamp
    row_marker *m = reinterpret_cast<row_marker *>(ti->buf_);
    m->marker_type_ = row_marker::mt_remove;
    val_ = Str(ti->buf_, sizeof(*m));
}

template <typename R>
void replay_query<R>::apply(R*& value, bool has_value, threadinfo* ti) {
    R** cur_value = &value;
    if (!has_value)
	*cur_value = 0;

    // find point to insert change (may be after some delta markers)
    while (*cur_value && row_is_delta_marker(*cur_value)
	   && (*cur_value)->timestamp() > qtimes_.ts)
	cur_value = &row_get_delta_marker(*cur_value)->prev_;

    // check out of date
    if (*cur_value && (*cur_value)->timestamp() >= qtimes_.ts)
	return;

    // if not modifying, delete everything earlier
    if (qt_ != QT_Replay_Modify)
	while (R* old_value = *cur_value) {
	    if (row_is_delta_marker(old_value)) {
		ti->pstat.mark_delta_removed();
		*cur_value = row_get_delta_marker(old_value)->prev_;
	    } else
		*cur_value = 0;
	    old_value->deallocate(*ti);
	}

    // actually apply change
    if (qt_ == QT_Replay_Put1)
        *cur_value = R::create1(val_, qtimes_.ts, *ti);
    else if (qt_ != QT_Replay_Modify) {
        serial_changeset<typename R::index_type> changeset(val_);
	*cur_value = R::create(changeset, qtimes_.ts, *ti);
    } else {
	if (*cur_value && (*cur_value)->timestamp() == qtimes_.prev_ts) {
	    R* old_value = *cur_value;
            serial_changeset<typename R::index_type> changeset(val_);
	    *cur_value = old_value->update(changeset, qtimes_.ts, *ti);
	    if (*cur_value != old_value)
		old_value->deallocate(*ti);
	} else {
	    // XXX assume that memory exists before saved request -- it does
	    // in conventional log replay, but that's an ugly interface
	    val_.s -= sizeof(row_delta_marker<R>);
	    val_.len += sizeof(row_delta_marker<R>);
	    R* new_value = R::create1(val_, qtimes_.ts | 1, *ti);
	    row_delta_marker<R>* dm = row_get_delta_marker(new_value, true);
	    dm->marker_type_ = row_marker::mt_delta;
	    dm->prev_ts_ = qtimes_.prev_ts;
	    dm->prev_ = *cur_value;
	    *cur_value = new_value;
	    ti->pstat.mark_delta_created();
	}
    }

    // clean up
    while (value && row_is_delta_marker(value)) {
	R **prev = 0, **trav = &value;
	while (*trav && row_is_delta_marker(*trav)) {
	    prev = trav;
	    trav = &row_get_delta_marker(*trav)->prev_;
	}
	if (prev && *trav
	    && row_get_delta_marker(*prev)->prev_ts_ == (*trav)->timestamp()) {
	    R *old_prev = *prev;
	    Str req = old_prev->col(0);
	    req.s += sizeof(row_delta_marker<R>);
	    req.len -= sizeof(row_delta_marker<R>);
	    serial_changeset<typename R::index_type> changeset(req);
	    *prev = (*trav)->update(changeset, old_prev->timestamp() - 1, *ti);
	    if (*prev != *trav)
		(*trav)->deallocate(*ti);
	    old_prev->deallocate(*ti);
	    ti->pstat.mark_delta_removed();
	} else
	    break;
    }
}

#endif
