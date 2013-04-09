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
#ifndef MISC_HH
#define MISC_HH
#include "compiler.hh"
#include "shared_config.hh"
#include "str.hh"
#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#include <math.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <stdarg.h>

#if HAVE_INT64_T_IS_LONG_LONG
#define PRIuKVTS "llu"
#else
#define PRIuKVTS "lu"
#endif
#define PRIKVTSPARTS "%lu.%06lu"

#define KVTS_HIGHPART(t) ((unsigned long) ((t) >> 32))
#define KVTS_LOWPART(t) ((unsigned long) (uint32_t) (t))

struct spinlock {
  uint8_t locked;   // Is the lock held?
};

inline kvtimestamp_t timestamp() {
    struct timeval tv;
    gettimeofday(&tv, 0);
    return ((kvtimestamp_t) tv.tv_sec << 32) | (unsigned int)tv.tv_usec;
}

inline kvtimestamp_t timestamp_sub(kvtimestamp_t a, kvtimestamp_t b) {
    a -= b;
    if (KVTS_LOWPART(a) > 999999)
	a -= ((kvtimestamp_t) 1 << 32) - 1000000;
    return a;
}

inline void xalarm(double d) {
    double ip, fp = modf(d, &ip);
    struct itimerval x;
    timerclear(&x.it_interval);
    x.it_value.tv_sec = (long) ip;
    x.it_value.tv_usec = (long) (fp * 1000000);
    setitimer(ITIMER_REAL, &x, 0);
}

inline double now()
{
  struct timeval tv;
  gettimeofday(&tv, 0);
  return tv.tv_sec + tv.tv_usec / 1000000.0;
}

inline struct timespec &set_timespec(struct timespec &x, double y)
{
    double ipart = floor(y);
    x.tv_sec = (long) ipart;
    x.tv_nsec = (long) ((y - ipart) * 1e9);
    return x;
}

inline void napms(int n) /* nap n milliseconds */
{
  int ret;
  struct timespec req, rem;

  req.tv_sec = n / 1000;
  req.tv_nsec = (n % 1000) * 1000000;
  ret = nanosleep(&req, &rem);
  if(ret == -1 && errno != EINTR){
    perror("nanosleep");
    exit(EXIT_FAILURE);
  }
}

inline void initlock(struct spinlock *lock)
{
  lock->locked = 0;
}

// Acquire the lock.
// Loops (spins) until the lock is acquired.
// Holding a lock for a long time may cause
// other CPUs to waste time spinning to acquire it.
inline void acquire(struct spinlock *lock)
{
  // The xchg is atomic.
  // It also serializes, so that reads after acquire are not
  // reordered before it.
  while(xchg(&lock->locked, (uint8_t)1) == 1)
    ;
}

// Release the lock.
inline void release(struct spinlock *lock)
{
  // The xchg serializes, so that reads before release are
  // not reordered after it.  (This reordering would be allowed
  // by the Intel manuals, but does not happen on current
  // Intel processors.  The xchg being asm volatile also keeps
  // gcc from delaying the above assignments.)
  xchg(&lock->locked, (uint8_t)0);
}

struct quick_istr {
    char buf_[32];
    char *bbuf_;
    quick_istr() {
      set(0);
    }
    quick_istr(unsigned long x, int minlen = 0) {
      set(x, minlen);
    }
    void set(unsigned long x, int minlen = 0){
	bbuf_ = buf_ + sizeof(buf_) - 1;
	do {
	    *--bbuf_ = (x % 10) + '0';
	    x /= 10;
	} while (--minlen > 0 || x != 0);
    }
    Str string() const {
	return Str(bbuf_, buf_ + sizeof(buf_) - 1);
    }
    const char *c_str() {
	buf_[sizeof(buf_) - 1] = 0;
	return bbuf_;
    }
    bool operator==(Str s) const {
	return s.len == (buf_ + sizeof(buf_) - 1) - bbuf_
	    && memcmp(s.s, bbuf_, s.len) == 0;
    }
    bool operator!=(Str s) const {
	return !(*this == s);
    }
};

size_t get_hugepage_size();

struct ckstate;
class threadinfo;
void checkpoint1(ckstate *c, Str key, const row_type *row);

struct Clp_Parser;
int clp_parse_suffixdouble(struct Clp_Parser *clp, const char *vstr,
			   int complain, void *user_data);

#endif
