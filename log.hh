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
#include "kvdconfig.hh"
#include "str.hh"
#include "string.hh"
#include "circular_int.hh"
#include <pthread.h>
class kvtable;
class threadinfo;
template <typename R> struct query;
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
    void log_query(int command, const query_times& qt, Str key, Str value);

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

    uint64_t replayandclean1(query<row_type> &q,
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

#endif
