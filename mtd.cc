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
// -*- mode: c++ -*-
// kvd: key/value server
//

#include <stdio.h>
#include <stdarg.h>
#include <ctype.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <limits.h>
#if HAVE_SYS_EPOLL_H
#include <sys/epoll.h>
#endif
#if __linux__
#include <asm-generic/mman.h>
#endif
#include <fcntl.h>
#include <assert.h>
#include <string.h>
#include <pthread.h>
#include <math.h>
#include <signal.h>
#include <errno.h>
#ifdef __linux__
#include <malloc.h>
#endif
#include "kvstats.hh"
#include "json.hh"
#include "kvtest.hh"
#include "kvrandom.hh"
#include "clp.h"
#include "log.hh"
#include "checkpoint.hh"
#include "file.hh"
#include "kvproto.hh"
#include "masstree_query.hh"
#include <algorithm>

enum { CKState_Quit, CKState_Uninit, CKState_Ready, CKState_Go };

volatile bool timeout[2] = {false, false};
double duration[2] = {10, 0};

Masstree::default_table *tree;

// all default to the number of cores
static int udpthreads = 0;
static int tcpthreads = 0;
static int nckthreads = 0;
static int testthreads = 0;
static int nlogger = 0;
static std::vector<int> cores;

static bool logging = true;
static bool pinthreads = false;
static bool recovery_only = false;
volatile uint64_t globalepoch = 1;     // global epoch, updated by main thread regularly
static int port = 2117;
static uint64_t test_limit = ~uint64_t(0);
static int doprint = 0;

static volatile sig_atomic_t go_quit = 0;
static int quit_pipe[2];

static const char *logdir[MaxCores];
static const char *ckpdir[MaxCores];
static int nlogdir = 0;
static int nckpdir = 0;

static logset* logs;
volatile bool recovering = false; // so don't add log entries, and free old value immediately

static double checkpoint_interval = 1000000;
static kvepoch_t ckp_gen = 0; // recover from checkpoint
static ckstate *cks = NULL; // checkpoint status of all checkpointing threads
static pthread_cond_t rec_cond;
pthread_mutex_t rec_mu;
static int rec_nactive;
static int rec_state = REC_NONE;

kvtimestamp_t initial_timestamp;

static pthread_cond_t checkpoint_cond;
static pthread_mutex_t checkpoint_mu;
static struct ckstate *cktable;

static void prepare_thread(threadinfo *ti);
static void *tcpgo(void *);
static void *udpgo(void *);
static int handshake(struct kvin *kvin, struct kvout *kvout, threadinfo *ti, bool &ok);
static int onego(query<row_type> &q, struct kvin *kvin, struct kvout *kvout, reqst_machine &rsm, threadinfo *ti);

static void log_init();
static void recover(threadinfo *);
static kvepoch_t read_checkpoint(threadinfo *, const char *path);
static uint64_t traverse_checkpoint_inorder(uint64_t off, uint64_t n,
                                            char *base, uint64_t *ind,
                                            uint64_t max, threadinfo *ti);

static void *conc_checkpointer(void *);
static void recovercheckpoint(threadinfo *ti);

static void *canceling(void *);
static void catchint(int);
static void epochinc(int);
static void print_stat();

/* running local tests */
void test_timeout(int) {
    size_t n;
    for (n = 0; n < arraysize(timeout) && timeout[n]; ++n)
	/* do nothing */;
    if (n < arraysize(timeout)) {
	timeout[n] = true;
	if (n + 1 < arraysize(timeout) && duration[n + 1])
	    xalarm(duration[n + 1]);
    }
}

struct kvtest_client {
    kvtest_client()
	: checks_(0), kvo_() {
    }
    kvtest_client(const char *testname)
	: testname_(testname), checks_(0), kvo_() {
    }

    int id() const {
	return ti_->ti_index;
    }
    int nthreads() const {
	return testthreads;
    }
    void set_thread(threadinfo *ti) {
	ti_ = ti;
    }
    void register_timeouts(int n) {
	mandatory_assert(n <= (int) arraysize(::timeout));
	for (int i = 1; i < n; ++i)
	    if (duration[i] == 0)
		duration[i] = 0;//duration[i - 1];
    }
    bool timeout(int which) const {
	return ::timeout[which];
    }
    uint64_t limit() const {
	return test_limit;
    }
    double now() const {
	return ::now();
    }

    void get(long ikey, Str *value);
    void get(const Str &key);
    void get(long ikey) {
	quick_istr key(ikey);
	get(key.string());
    }
    void get_check(const Str &key, const Str &expected);
    void get_check(const char *key, const char *expected) {
        get_check(Str(key, strlen(key)), Str(expected, strlen(expected)));
    }
    void get_check(long ikey, long iexpected) {
	quick_istr key(ikey), expected(iexpected);
	get_check(key.string(), expected.string());
    }
    void get_check_key8(long ikey, long iexpected) {
	quick_istr key(ikey, 8), expected(iexpected);
	get_check(key.string(), expected.string());
    }
    void get_check_key10(long ikey, long iexpected) {
	quick_istr key(ikey, 10), expected(iexpected);
	get_check(key.string(), expected.string());
    }
    void get_col_check(const Str &key, int col, const Str &expected);
    void get_col_check_key10(long ikey, int col, long iexpected) {
	quick_istr key(ikey, 10), expected(iexpected);
	get_col_check(key.string(), col, expected.string());
    }
    bool get_sync(long ikey);

    void put(const Str &key, const Str &value);
    void put(const char *key, const char *val) {
        put(Str(key, strlen(key)), Str(val, strlen(val)));
    }
    void put(long ikey, long ivalue) {
	quick_istr key(ikey), value(ivalue);
	put(key.string(), value.string());
    }
    void put_key8(long ikey, long ivalue) {
	quick_istr key(ikey, 8), value(ivalue);
	put(key.string(), value.string());
    }
    void put_key10(long ikey, long ivalue) {
	quick_istr key(ikey, 10), value(ivalue);
	put(key.string(), value.string());
    }
    void put_col(const Str &key, int col, const Str &value);
    void put_col_key10(long ikey, int col, long ivalue) {
	quick_istr key(ikey, 10), value(ivalue);
	put_col(key.string(), col, value.string());
    }

    bool remove_sync(long ikey);

    void puts_done() {
    }
    void wait_all() {
    }
    void rcu_quiesce() {
    }
    String make_message(StringAccum &sa) const;
    void notice(const char *fmt, ...);
    void fail(const char *fmt, ...);
    void report(const Json &result) {
	json_.merge(result);
	fprintf(stderr, "%d: %s\n", ti_->ti_index, json_.unparse().c_str());
    }
    threadinfo *ti_;
    query<row_type> q_[10];
    const char *testname_;
    kvrandom_lcg_nr rand;
    int checks_;
    Json json_;
    struct kvout *kvo_;
    static volatile int failing;
};

volatile int kvtest_client::failing;

void kvtest_client::get(long ikey, Str *value)
{
    quick_istr key(ikey);
    q_[0].begin_get1(key.string());
    if (tree->get(q_[0], ti_))
	*value = q_[0].get1_value();
    else
	*value = Str();
}

void kvtest_client::get(const Str &key)
{
    q_[0].begin_get1(key);
    (void) tree->get(q_[0], ti_);
}

void kvtest_client::get_check(const Str &key, const Str &expected)
{
    q_[0].begin_get1(key);
    if (!tree->get(q_[0], ti_)) {
	fail("get(%.*s) failed (expected %.*s)\n", key.len, key.s, expected.len, expected.s);
        return;
    }
    Str val = q_[0].get1_value();
    if (val.len != expected.len || memcmp(val.s, expected.s, val.len) != 0)
	fail("get(%.*s) returned unexpected value %.*s (expected %.*s)\n", key.len, key.s,
	     std::min(val.len, 40), val.s, std::min(expected.len, 40), expected.s);
    else
	++checks_;
}

void kvtest_client::get_col_check(const Str &key, int col, const Str &expected)
{
    q_[0].begin_get1(key, col);
    if (!tree->get(q_[0], ti_)) {
	fail("get.%d(%.*s) failed (expected %.*s)\n", col, key.len, key.s,
	     expected.len, expected.s);
        return;
    }
    Str val = q_[0].get1_value();
    if (val.len != expected.len || memcmp(val.s, expected.s, val.len) != 0)
	fail("get.%d(%.*s) returned unexpected value %.*s (expected %.*s)\n",
	     col, key.len, key.s, std::min(val.len, 40), val.s,
	     std::min(expected.len, 40), expected.s);
    else
	++checks_;
}

bool kvtest_client::get_sync(long ikey) {
    quick_istr key(ikey);
    q_[0].begin_get1(key.string());
    return tree->get(q_[0], ti_);
}

void kvtest_client::put(const Str &key, const Str &value) {
    while (failing)
	/* do nothing */;
    q_[0].begin_replace(key, value);
    (void) tree->replace(q_[0], ti_);
    if (ti_->ti_log) // NB may block
	ti_->ti_log->record(logcmd_put1, q_[0].query_times(), key, value);
}

void kvtest_client::put_col(const Str &key, int col, const Str &value) {
    while (failing)
	/* do nothing */;
#if !KVDB_ROW_TYPE_STR
    if (!kvo_)
	kvo_ = new_kvout(-1, 2048);
    Str req = row_type::make_put_col_request(kvo_, col, value);
    q_[0].begin_put(key, req);
    (void) tree->put(q_[0], ti_);
    if (ti_->ti_log) // NB may block
	ti_->ti_log->record(logcmd_put, q_[0].query_times(), key, req);
#else
    (void) key, (void) col, (void) value;
    assert(0);
#endif
}

bool kvtest_client::remove_sync(long ikey) {
    quick_istr key(ikey);
    q_[0].begin_remove(key.string());
    bool removed = tree->remove(q_[0], ti_);
    if (removed && ti_->ti_log) // NB may block
	ti_->ti_log->record(logcmd_remove, q_[0].query_times(), key.string(), Str());
    return removed;
}

String kvtest_client::make_message(StringAccum &sa) const {
    const char *begin = sa.begin();
    while (begin != sa.end() && isspace((unsigned char) *begin))
	++begin;
    String s = String(begin, sa.end());
    if (!s.empty() && s.back() != '\n')
	s += '\n';
    return s;
}

void kvtest_client::notice(const char *fmt, ...) {
    va_list val;
    va_start(val, fmt);
    String m = make_message(StringAccum().vsnprintf(500, fmt, val));
    va_end(val);
    if (m)
	fprintf(stderr, "%d: %s", ti_->ti_index, m.c_str());
}

void kvtest_client::fail(const char *fmt, ...) {
    static spinlock failing_lock = {0};
    static spinlock fail_message_lock = {0};
    static String fail_message;
    failing = 1;

    va_list val;
    va_start(val, fmt);
    String m = make_message(StringAccum().vsnprintf(500, fmt, val));
    va_end(val);
    if (!m)
	m = "unknown failure";

    acquire(&fail_message_lock);
    if (fail_message != m) {
	fail_message = m;
	fprintf(stderr, "%d: %s", ti_->ti_index, m.c_str());
    }
    release(&fail_message_lock);

    if (doprint) {
	acquire(&failing_lock);
	fprintf(stdout, "%d: %s", ti_->ti_index, m.c_str());
	tree->print(stdout, 0);
	fflush(stdout);
    }

    mandatory_assert(0);
}

static void *testgo(void *arg) {
    kvtest_client *kc = (kvtest_client *) arg;
    prepare_thread(kc->ti_);

    if (strcmp(kc->testname_, "rw1") == 0)
	kvtest_rw1(*kc);
    else if (strcmp(kc->testname_, "rw2") == 0)
	kvtest_rw2(*kc);
    else if (strcmp(kc->testname_, "rw3") == 0)
	kvtest_rw3(*kc);
    else if (strcmp(kc->testname_, "rw4") == 0)
	kvtest_rw4(*kc);
    else if (strcmp(kc->testname_, "rwsmall24") == 0)
	kvtest_rwsmall24(*kc);
    else if (strcmp(kc->testname_, "rwsep24") == 0)
	kvtest_rwsep24(*kc);
    else if (strcmp(kc->testname_, "palma") == 0)
        kvtest_palma(*kc);
    else if (strcmp(kc->testname_, "palmb") == 0)
        kvtest_palmb(*kc);
    else if (strcmp(kc->testname_, "rw16") == 0)
        kvtest_rw16(*kc);
    else if (strcmp(kc->testname_, "rw5") == 0
	     || strcmp(kc->testname_, "rw1fixed") == 0)
        kvtest_rw1fixed(*kc);
    else if (strcmp(kc->testname_, "ycsbk") == 0)
        kvtest_ycsbk(*kc);
    else if (strcmp(kc->testname_, "wd1") == 0)
	kvtest_wd1(10000000, 1, *kc);
    else if (strcmp(kc->testname_, "wd1check") == 0)
	kvtest_wd1_check(10000000, 1, *kc);
    else if (strcmp(kc->testname_, "w1") == 0)
	kvtest_w1_seed(*kc, 31949 + kc->id());
    else if (strcmp(kc->testname_, "r1") == 0)
	kvtest_r1_seed(*kc, 31949 + kc->id());
    else if (strcmp(kc->testname_, "wcol1") == 0)
	kvtest_wcol1(*kc, 31949 + kc->id() % 48, 5000000);
    else if (strcmp(kc->testname_, "rcol1") == 0)
	kvtest_rcol1(*kc, 31949 + kc->id() % 48, 5000000);
    else
	kc->fail("unknown test '%s'", kc->testname_);
    return 0;
}

static const char * const kvstats_name[] = {
    "ops", "ops_per_sec", "puts", "gets", "scans", "puts_per_sec", "gets_per_sec", "scans_per_sec"
};

void runtest(const char *testname, int nthreads) {
    std::vector<kvtest_client> clients(nthreads, kvtest_client(testname));
    ::testthreads = nthreads;
    for (int i = 0; i < nthreads; ++i)
	clients[i].set_thread(threadinfo::make(threadinfo::TI_PROCESS, i));
    bzero((void *)timeout, sizeof(timeout));
    signal(SIGALRM, test_timeout);
    if (duration[0])
	xalarm(duration[0]);
    for (int i = 0; i < nthreads; ++i) {
	int r = pthread_create(&clients[i].ti_->ti_threadid, 0, testgo, &clients[i]);
	mandatory_assert(r == 0);
    }
    for (int i = 0; i < nthreads; ++i)
	pthread_join(clients[i].ti_->ti_threadid, 0);

    kvstats kvs[arraysize(kvstats_name)];
    for (int i = 0; i < nthreads; ++i)
	for (int j = 0; j < (int) arraysize(kvstats_name); ++j)
	    if (double x = clients[i].json_.get_d(kvstats_name[j]))
		kvs[j].add(x);
    for (int j = 0; j < (int) arraysize(kvstats_name); ++j)
	kvs[j].print_report(kvstats_name[j]);
}


/* main loop */

enum { clp_val_suffixdouble = Clp_ValFirstUser };
enum { opt_nolog = 1, opt_pin, opt_logdir, opt_port, opt_ckpdir, opt_duration,
       opt_test, opt_test_name, opt_threads, opt_cores,
       opt_print, opt_norun, opt_checkpoint, opt_limit };
static const Clp_Option options[] = {
    { "no-log", 0, opt_nolog, 0, 0 },
    { 0, 'n', opt_nolog, 0, 0 },
    { "no-run", 0, opt_norun, 0, 0 },
    { "pin", 'p', opt_pin, 0, Clp_Negate },
    { "logdir", 0, opt_logdir, Clp_ValString, 0 },
    { "ld", 0, opt_logdir, Clp_ValString, 0 },
    { "checkpoint", 'c', opt_checkpoint, Clp_ValDouble, Clp_Optional | Clp_Negate },
    { "ckp", 0, opt_checkpoint, Clp_ValDouble, Clp_Optional | Clp_Negate },
    { "ckpdir", 0, opt_ckpdir, Clp_ValString, 0 },
    { "ckdir", 0, opt_ckpdir, Clp_ValString, 0 },
    { "cd", 0, opt_ckpdir, Clp_ValString, 0 },
    { "port", 0, opt_port, Clp_ValInt, 0 },
    { "duration", 'd', opt_duration, Clp_ValDouble, 0 },
    { "limit", 'l', opt_limit, clp_val_suffixdouble, 0 },
    { "test", 0, opt_test, Clp_ValString, 0 },
    { "test-rw1", 0, opt_test_name, 0, 0 },
    { "test-rw2", 0, opt_test_name, 0, 0 },
    { "test-rw3", 0, opt_test_name, 0, 0 },
    { "test-rw4", 0, opt_test_name, 0, 0 },
    { "test-rw5", 0, opt_test_name, 0, 0 },
    { "test-rw16", 0, opt_test_name, 0, 0 },
    { "test-palm", 0, opt_test_name, 0, 0 },
    { "test-ycsbk", 0, opt_test_name, 0, 0 },
    { "test-rw1fixed", 0, opt_test_name, 0, 0 },
    { "threads", 'j', opt_threads, Clp_ValInt, 0 },
    { "cores", 0, opt_cores, Clp_ValString, 0 },
    { "print", 0, opt_print, 0, Clp_Negate }
};

int
main(int argc, char *argv[])
{
  int s, ret, yes = 1, i = 1, firstcore = -1, corestride = 1;
  const char *dotest = 0;
  nlogger = tcpthreads = udpthreads = nckthreads = sysconf(_SC_NPROCESSORS_ONLN);
  Clp_Parser *clp = Clp_NewParser(argc, argv, (int) arraysize(options), options);
  Clp_AddType(clp, clp_val_suffixdouble, Clp_DisallowOptions, clp_parse_suffixdouble, 0);
  int opt;
  while ((opt = Clp_Next(clp)) >= 0) {
      switch (opt) {
      case opt_nolog:
	  logging = false;
	  break;
      case opt_pin:
	  pinthreads = !clp->negated;
	  break;
      case opt_threads:
	  nlogger = tcpthreads = udpthreads = nckthreads = clp->val.i;
	  break;
      case opt_logdir: {
	  const char *s = strtok((char *) clp->vstr, ",");
	  while (s) {
	      mandatory_assert(nlogdir < MaxCores);
	      logdir[nlogdir++] = s;
	      s = strtok(NULL, ",");
	  }
	  break;
      }
      case opt_ckpdir: {
	  const char *s = strtok((char *) clp->vstr, ",");
	  while (s) {
	      mandatory_assert(nckpdir < MaxCores);
	      ckpdir[nckpdir++] = s;
	      s = strtok(NULL, ",");
	  }
	  break;
      }
      case opt_checkpoint:
	  if (clp->negated || (clp->have_val && clp->val.d <= 0))
	      checkpoint_interval = -1;
	  else if (clp->have_val)
	      checkpoint_interval = clp->val.d;
	  else
	      checkpoint_interval = 30;
	  break;
      case opt_port:
	  port = clp->val.i;
	  break;
      case opt_duration:
	  duration[0] = clp->val.d;
	  break;
      case opt_limit:
	  test_limit = (uint64_t) clp->val.d;
	  break;
      case opt_test:
	  dotest = clp->vstr;
	  break;
      case opt_test_name:
	  dotest = clp->option->long_name + 5;
	  break;
      case opt_print:
	  doprint = !clp->negated;
	  break;
      case opt_cores:
	  if (firstcore >= 0 || cores.size() > 0) {
	      Clp_OptionError(clp, "%<%O%> already given");
	      exit(EXIT_FAILURE);
	  } else {
	      const char *plus = strchr(clp->vstr, '+');
	      Json ij = Json::parse(clp->vstr),
		  aj = Json::parse(String("[") + String(clp->vstr) + String("]")),
		  pj1 = Json::parse(plus ? String(clp->vstr, plus) : "x"),
		  pj2 = Json::parse(plus ? String(plus + 1) : "x");
	      for (int i = 0; aj && i < aj.size(); ++i)
		  if (!aj[i].is_int() || aj[i].to_i() < 0)
		      aj = Json();
	      if (ij && ij.is_int() && ij.to_i() >= 0)
		  firstcore = ij.to_i(), corestride = 1;
	      else if (pj1 && pj2 && pj1.is_int() && pj1.to_i() >= 0 && pj2.is_int())
		  firstcore = pj1.to_i(), corestride = pj2.to_i();
	      else if (aj) {
		  for (int i = 0; i < aj.size(); ++i)
		      cores.push_back(aj[i].to_i());
	      } else {
		  Clp_OptionError(clp, "bad %<%O%>, expected %<CORE1%>, %<CORE1+STRIDE%>, or %<CORE1,CORE2,...%>");
		  exit(EXIT_FAILURE);
	      }
	  }
	  break;
      case opt_norun:
	  recovery_only = true;
	  break;
      default:
	  fprintf(stderr, "Usage: kvd [-np] [--ld dir1[,dir2,...]] [--cd dir1[,dir2,...]]\n");
	  exit(EXIT_FAILURE);
      }
  }
  Clp_DeleteParser(clp);
  Perf::stat::initmain(pinthreads);
  if (nlogdir == 0) {
    logdir[0] = ".";
    nlogdir = 1;
  }
  if (nckpdir == 0) {
    ckpdir[0] = ".";
    nckpdir = 1;
  }
  if (firstcore < 0)
      firstcore = cores.size() ? cores.back() + 1 : 0;
  for (; (int) cores.size() < udpthreads; firstcore += corestride)
      cores.push_back(firstcore);

  // for -pg profiling
  signal(SIGINT, catchint);

  // log epoch starts at 1
  global_log_epoch = 1;
  global_wake_epoch = 0;
  log_epoch_interval.tv_sec = 0;
  log_epoch_interval.tv_usec = 200000;

  // increment the global epoch every second
  if (!dotest) {
      signal(SIGALRM, epochinc);
      struct itimerval etimer;
      etimer.it_interval.tv_sec = 1;
      etimer.it_interval.tv_usec = 0;
      etimer.it_value.tv_sec = 1;
      etimer.it_value.tv_usec = 0;
      ret = setitimer(ITIMER_REAL, &etimer, NULL);
      mandatory_assert(ret == 0);
  }

  // arrange for a per-thread threadinfo pointer
  ret = pthread_key_create(&threadinfo::key, 0);
  mandatory_assert(ret == 0);

  // for parallel recovery
  ret = pthread_cond_init(&rec_cond, 0);
  mandatory_assert(ret == 0);
  ret = pthread_mutex_init(&rec_mu, 0);
  mandatory_assert(ret == 0);

  // for waking up the checkpoint thread
  ret = pthread_cond_init(&checkpoint_cond, 0);
  mandatory_assert(ret == 0);
  ret = pthread_mutex_init(&checkpoint_mu, 0);
  mandatory_assert(ret == 0);

  threadinfo *main_ti = threadinfo::make(threadinfo::TI_MAIN, -1);
  main_ti->enter();

  initial_timestamp = timestamp();
  tree = new Masstree::default_table;
  tree->initialize(main_ti);
  printf("%s, %s, pin-threads %s, ", tree->name(), row_type::name(),
         pinthreads ? "enabled" : "disabled");
  if(logging){
    printf("logging enabled\n");
    log_init();
    recover(main_ti);
  } else {
    printf("logging disabled\n");
  }

  // UDP threads, each with its own port.
  if (udpthreads == 0)
      printf("0 udp threads\n");
  else if (udpthreads == 1)
      printf("1 udp thread (port %d)\n", port);
  else
      printf("%d udp threads (ports %d-%d)\n", udpthreads, port, port + udpthreads - 1);
  for(i = 0; i < udpthreads; i++){
    threadinfo *ti = threadinfo::make(threadinfo::TI_PROCESS, i);
    ret = pthread_create(&ti->ti_threadid, 0, udpgo, ti);
    mandatory_assert(ret == 0);
  }

  if (dotest) {
      if (strcmp(dotest, "palm") == 0) {
        runtest("palma", 1);
        runtest("palmb", tcpthreads);
      } else
        runtest(dotest, tcpthreads);
      print_stat();
      tree->stats(stderr);
      if (doprint)
	  tree->print(stdout, 0);
      exit(0);
  }

  // TCP socket and threads

  s = socket(AF_INET, SOCK_STREAM, 0);
  mandatory_assert(s >= 0);
  setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
  setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

  struct sockaddr_in sin;
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = INADDR_ANY;
  sin.sin_port = htons(port);
  ret = bind(s, (struct sockaddr *) &sin, sizeof(sin));
  if (ret < 0) {
      perror("bind");
      exit(EXIT_FAILURE);
  }

  ret = listen(s, 100);
  if (ret < 0) {
      perror("listen");
      exit(EXIT_FAILURE);
  }

  threadinfo **tcpti = new threadinfo *[tcpthreads];
  printf("%d tcp threads (port %d)\n", tcpthreads, port);
  for(i = 0; i < tcpthreads; i++){
    threadinfo *ti = threadinfo::make(threadinfo::TI_PROCESS, i);
    ret = pipe(ti->ti_pipe);
    mandatory_assert(ret == 0);
    ret = pthread_create(&ti->ti_threadid, 0, tcpgo, ti);
    mandatory_assert(ret == 0);
    tcpti[i] = ti;
  }
  // Create a canceling thread.
  ret = pipe(quit_pipe);
  assert(ret == 0);
  pthread_t tid;
  pthread_create(&tid, NULL, canceling, NULL);

  static int next = 0;
  while(1){
    int s1;
    struct sockaddr_in sin1;
    socklen_t sinlen = sizeof(sin1);

    bzero(&sin1, sizeof(sin1));
    s1 = accept(s, (struct sockaddr *) &sin1, &sinlen);
    mandatory_assert(s1 >= 0);
    // Bind the connection to a particular core if required.
    int target_core;
    if (read(s1, &target_core, sizeof(target_core)) != sizeof(target_core)) {
        perror("read");
        continue;
    }
    setsockopt(s1, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

    threadinfo *ti;
    if (target_core == -1) {
        ti = tcpti[next % tcpthreads];
        ++next;
    } else {
        assert(pinthreads && target_core < tcpthreads);
        ti = tcpti[target_core];
    }
    ssize_t w = write(ti->ti_pipe[1], &s1, sizeof(s1));
    mandatory_assert((size_t) w == sizeof(s1));
  }
}

void
catchint(int)
{
    go_quit = 1;
    char cmd = 0;
    // Does not matter if the write fails (when the pipe is full)
    int r = write(quit_pipe[1], &cmd, sizeof(cmd));
    (void)r;
}

inline const char *threadtype(int type) {
  switch (type) {
    case threadinfo::TI_MAIN:
      return "main";
    case threadinfo::TI_PROCESS:
      return "process";
    case threadinfo::TI_LOG:
      return "log";
    case threadinfo::TI_CHECKPOINT:
      return "checkpoint";
    default:
      mandatory_assert(0 && "Unknown threadtype");
      break;
  };
}

void
print_stat()
{
    int nstat = 0;
    const Perf::stat *allstat[MaxCores * 2];
    bzero(allstat, sizeof(allstat));
    for (threadinfo *ti = threadinfo::allthreads; ti; ti = ti->ti_next)
        if (ti->ti_purpose == threadinfo::TI_PROCESS)
            allstat[nstat ++] = &ti->pstat;
    Perf::stat::print(allstat, nstat);
}

void *
canceling(void *)
{
    char cmd;
    int r = read(quit_pipe[0], &cmd, sizeof(cmd));
    (void) r;
    assert(r == sizeof(cmd) && cmd == 0);
    // Cancel wake up checkpointing threads
    pthread_mutex_lock(&checkpoint_mu);
    pthread_cond_signal(&checkpoint_cond);
    pthread_mutex_unlock(&checkpoint_mu);

    pthread_t me = pthread_self();
    fprintf(stderr, "\n");
    // cancel outstanding threads. Checkpointing threads will exit safely
    // when the checkpointing thread 0 sees go_quit, and don't need cancel
    for (threadinfo *ti = threadinfo::allthreads; ti; ti = ti->ti_next)
        if (ti->ti_purpose != threadinfo::TI_MAIN
            && ti->ti_purpose != threadinfo::TI_CHECKPOINT
	    && !pthread_equal(me, ti->ti_threadid)) {
            int r = pthread_cancel(ti->ti_threadid);
            mandatory_assert(r == 0);
        }

    // join canceled threads
    for (threadinfo *ti = threadinfo::allthreads; ti; ti = ti->ti_next)
        if (ti->ti_purpose != threadinfo::TI_MAIN
	    && !pthread_equal(me, ti->ti_threadid)) {
            fprintf(stderr, "joining thread %s:%d\n",
                    threadtype(ti->ti_purpose), ti->ti_index);
            int r = pthread_join(ti->ti_threadid, 0);
            mandatory_assert(r == 0);
        }
    print_stat();
    tree->stats(stderr);
    exit(0);
}

void
epochinc(int)
{
    globalepoch += 2;
}

// lookups in immutable binary-search table,
// created by checkpoint.
// this turns out to be slower than THREEWIDETREE,
// presumably since the latter has half as many levels.
bool
table_lookup(const Str &key, Str &val)
{
  if(cktable == 0)
    return false;

  uint64_t n = cktable->count;
  char *keys = cktable->keys->buf;
  char *keyvals = cktable->vals->buf;
  uint64_t *ind = (uint64_t *) cktable->ind->buf;

  uint64_t min = 0;
  uint64_t max = n - 1;
  do {
    uint64_t mid = min + (max - min) / 2;
    // assert(mid >= 0 && mid >= min && mid < n && mid <= max);
    int x;
    if(key.len < CkpKeyPrefixLen){
      x = strcmp(key.s, keys + mid*CkpKeyPrefixLen);
    } else {
      x = strncmp(key.s, keys + mid*CkpKeyPrefixLen, CkpKeyPrefixLen);
      if(x == 0)
        x = strcmp(key.s, keyvals + ind[mid]); // XXX should use keys[]
    }
    if(x == 0){
      char *p = keyvals + ind[mid] + key.len + 1 + sizeof(kvtimestamp_t);
      val.assign(p + sizeof(int), *(int *)p);
      return true;
    } else if(x < 0){
      max = mid - 1;
    } else {
      min = mid + 1;
    }
  } while(min <= max);

  return false;
}

// Return 1 if success, -1 if I/O error or protocol unmatch
int
handshake(struct kvin *kvin, struct kvout *kvout, threadinfo *ti, bool &ok)
{
  struct kvproto kvproto;
  if (KVR(kvin, kvproto) != sizeof(kvproto))
      return -1;
  ok = kvproto_check(kvproto);
  KVW(kvout, ok);
  KVW(kvout, ti->ti_index);
  return ok ? 1 : -1;
}

// returns 2 if the request is incompleted, 1 if one request is
// ready, -1 on error
int
tryreadreq(struct kvin *kvin, reqst_machine &rsm)
{
    int avail = kvcheck(kvin, 0);
    while (avail > 0) {
        int c = std::min(avail, rsm.wanted);
        if (kvread(kvin, rsm.p, c) != c)
            return -1;
        rsm.p += c;
        rsm.wanted -= c;
        if (rsm.wanted)
            return 2;
        avail -= c;
        switch (rsm.ci) {
        case CI_Cmd:
            assert(rsm.cmd > Cmd_None && rsm.cmd < Cmd_Max);
            rsm.goto_seq();
            break;
        case CI_Seq:
            if (rsm.cmd == Cmd_Checkpoint)
                return 1;
            rsm.goto_keylen();
            break;
        case CI_Keylen:
            rsm.goto_key();
            break;
        case CI_Key:
            if (rsm.cmd == Cmd_Remove)
                return 1;
            rsm.goto_reqlen();
            break;
        case CI_Reqlen:
            rsm.goto_req();
            break;
        case CI_Req:
            if (rsm.cmd != Cmd_Scan)
                return 1;
            rsm.goto_numpairs();
            break;
        case CI_Numpairs:
            assert(rsm.cmd == Cmd_Scan);
            return 1;
        default:
            assert(0 && "Bad component index");
        }
    }
    return 2;
}

// read one cmd from a kvin, execute it.
// returns 2 if the request is incompleted, 1 if one request is processed,
// -1 for error
int
onego(query<row_type> &q, struct kvin *kvin, struct kvout *kvout,
      reqst_machine &rsm, threadinfo *ti)
{
  int r;
  if ((r = tryreadreq(kvin, rsm)) != 1)
    return r;
  if(rsm.cmd == Cmd_Checkpoint){
    // force checkpoint
    pthread_mutex_lock(&checkpoint_mu);
    pthread_cond_broadcast(&checkpoint_cond);
    pthread_mutex_unlock(&checkpoint_mu);
  }
  else if(rsm.cmd == Cmd_Get){
    KVW(kvout, rsm.seq);
    q.begin_get(Str(rsm.key, rsm.keylen), Str(rsm.req, rsm.reqlen), kvout);
    // XXX: fix table_lookup, which should have its own row_type
    bool val_exists = tree->get(q, ti);
    if(!val_exists){
      //printf("no val for key %.*s\n", rsm.keylen, rsm.key);
      KVW(kvout, (short)-1);
    }
  } else if (rsm.cmd == Cmd_Put || rsm.cmd == Cmd_Put_Status) { // insert or update
      Str key(rsm.key, rsm.keylen), req(rsm.req, rsm.reqlen);
      q.begin_put(key, req);
      int status = tree->put(q, ti);
      if (ti->ti_log) // NB may block
	  ti->ti_log->record(logcmd_put, q.query_times(), key, req);
      KVW(kvout, rsm.seq);
      if (rsm.cmd == Cmd_Put_Status)
	  KVW(kvout, status);
  } else if(rsm.cmd == Cmd_Remove){ // remove
      Str key(rsm.key, rsm.keylen);
      q.begin_remove(key);
      bool removed = tree->remove(q, ti);
      if (removed && ti->ti_log) // NB may block
	  ti->ti_log->record(logcmd_remove, q.query_times(), key, Str());
      KVW(kvout, rsm.seq);
      KVW(kvout, (int) removed);
  } else {
    assert(rsm.cmd == Cmd_Scan);
    KVW(kvout, rsm.seq);
    if (rsm.numpairs > 0) {
      q.begin_scan(Str(rsm.key, rsm.keylen), rsm.numpairs, Str(rsm.req, rsm.reqlen), kvout);
      tree->scan(q, ti);
    }
    KVW(kvout, (int)0);
  }
  rsm.reset();
  return 1;
}

#if HAVE_SYS_EPOLL_H
struct tcpfds {
    int epollfd;

    tcpfds(int pipefd) {
	epollfd = epoll_create(10);
	if (epollfd < 0) {
	    perror("epoll_create");
	    exit(EXIT_FAILURE);
	}
	struct epoll_event ev;
	ev.events = EPOLLIN;
	ev.data.ptr = (void *) 1;
	int r = epoll_ctl(epollfd, EPOLL_CTL_ADD, pipefd, &ev);
	mandatory_assert(r == 0);
    }

    enum { max_events = 100 };
    typedef struct epoll_event eventset[max_events];
    int wait(eventset &es) {
	return epoll_wait(epollfd, es, max_events, -1);
    }

    conn *event_conn(eventset &es, int i) const {
	return (conn *) es[i].data.ptr;
    }

    void add(int fd, conn *c) {
	struct epoll_event ev;
	ev.events = EPOLLIN;
	ev.data.ptr = c;
	int r = epoll_ctl(epollfd, EPOLL_CTL_ADD, fd, &ev);
	mandatory_assert(r == 0);
    }

    void remove(int fd) {
	int r = epoll_ctl(epollfd, EPOLL_CTL_DEL, fd, NULL);
	mandatory_assert(r == 0);
    }
};
#else
class tcpfds {
    int pipefd_;
    int nfds_;
    fd_set rfds_;
    std::vector<conn *> conns_;

  public:
    tcpfds(int pipefd)
        : pipefd_(pipefd), nfds_(pipefd + 1) {
	mandatory_assert(pipefd < FD_SETSIZE);
	FD_ZERO(&rfds_);
	FD_SET(pipefd, &rfds_);
	conns_.resize(nfds_, 0);
	conns_[pipefd] = (conn *) 1;
    }

    typedef fd_set eventset;
    int wait(eventset &es) {
	es = rfds_;
	int r = select(nfds_, &es, 0, 0, 0);
        return r > 0 ? nfds_ : r;
    }

    conn *event_conn(eventset &es, int i) const {
	return FD_ISSET(i, &es) ? conns_[i] : 0;
    }

    void add(int fd, conn *c) {
	mandatory_assert(fd < FD_SETSIZE);
	FD_SET(fd, &rfds_);
	if (fd >= nfds_) {
	    nfds_ = fd + 1;
	    conns_.resize(nfds_, 0);
	}
	conns_[fd] = c;
    }

    void remove(int fd) {
	mandatory_assert(fd < FD_SETSIZE);
	FD_CLR(fd, &rfds_);
	if (fd == nfds_ - 1) {
	    while (nfds_ > 0 && !FD_ISSET(nfds_ - 1, &rfds_))
		--nfds_;
	}
    }
};
#endif

void
prepare_thread(threadinfo *ti)
{
    ti->enter();
#if __linux__
    if (pinthreads) {
	cpu_set_t cs;
	CPU_ZERO(&cs);
	CPU_SET(cores[ti->ti_index], &cs);
	mandatory_assert(sched_setaffinity(0, sizeof(cs), &cs) == 0);
    }
#else
    mandatory_assert(!pinthreads && "pinthreads not supported\n");
#endif
    if (logging)
	ti->ti_log = &logs->log(ti->ti_index % nlogger);
}

void *
tcpgo(void *xarg)
{
  threadinfo *ti = (threadinfo *) xarg;
  prepare_thread(ti);

  tcpfds sloop(ti->ti_pipe[0]);
  tcpfds::eventset events;
  query<row_type> q;

  while (1) {
    int nev = sloop.wait(events);
    for (int i = 0; i < nev; i++) {
      conn *c = sloop.event_conn(events, i);
      if (c == (conn *) 1) {
        // new connections
#define MAX_NEWCONN 100
        int ss[MAX_NEWCONN];
        ssize_t len = read(ti->ti_pipe[0], ss, sizeof(ss));
        mandatory_assert(len > 0 && len % sizeof(int) == 0);
        for (int j = 0; j * sizeof(int) < (size_t) len; ++j){
          struct conn *c = new conn(ss[j]);
          c->kvin = new_kvin(ss[j], 20*1024);
          c->kvout = new_kvout(ss[j], 20*1024);
          c->rsm.reset();
          c->ready = false;
          sloop.add(c->fd, c);
        }
      } else if (c) {
        // Should not block as suggested by epoll
        int ret = mayblock_kvoneread(c->kvin);
        if (unlikely(ret <= 0))
            goto closed;
        if (unlikely(!c->ready))
            ret = handshake(c->kvin, c->kvout, ti, c->ready);
        else {
          ti->rcu_start();
          while ((ret = onego(q, c->kvin, c->kvout, c->rsm, ti)) == 1)
              /* do nothing */;
          ti->rcu_stop();
        }
        if(ret >= 0) {
          kvflush(c->kvout);
          if(likely(c->ready))
            continue;
        }
        if (ret < 0)
          printf("socket read error\n");
closed:
	sloop.remove(c->fd);
        close(c->fd);
        free_kvin(c->kvin);
        free_kvout(c->kvout);
        free(c);
      }
    }
  }
  return 0;
}

// serve a client udp socket, in a dedicated thread
void *
udpgo(void *xarg)
{
  int ret;
  threadinfo *ti = (threadinfo *) xarg;
  prepare_thread(ti);

  struct sockaddr_in sin;
  bzero(&sin, sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_port = htons(port + ti->ti_index);

  int s = socket(AF_INET, SOCK_DGRAM, 0);
  mandatory_assert(s >= 0);
  ret = bind(s, (struct sockaddr *) &sin, sizeof(sin));
  mandatory_assert(ret == 0 && "bind failed");
  int sobuflen = 512*1024;
  setsockopt(s, SOL_SOCKET, SO_RCVBUF, &sobuflen, sizeof(sobuflen));

  int buflen = 4096;
  char *inbuf = (char *) malloc(buflen);
  mandatory_assert(inbuf);
  struct kvin *kvin = new_bufkvin(inbuf);
  struct kvout *kvout = new_bufkvout();

  reqst_machine rsm;
  query<row_type> q;
  while(1){
    struct sockaddr_in sin;
    socklen_t sinlen = sizeof(sin);
    ssize_t cc = recvfrom(s, inbuf, buflen, 0, (struct sockaddr *) &sin, &sinlen);
    if(cc < 0){
      perror("udpgo read");
      exit(EXIT_FAILURE);
    }
    kvin_setlen(kvin, cc);
    kvout_reset(kvout);
    rsm.reset();

    conn c(-1);
    c.kvin = NULL;
    c.kvout = kvout;

    // Fail if we received a partial request
    ti->rcu_start();
    if(onego(q, kvin, kvout, rsm, ti) == 1){
      if(kvout->n > 0){
        cc = sendto(s, kvout->buf, kvout->n, 0, (struct sockaddr *) &sin, sinlen);
        mandatory_assert(cc == (ssize_t) kvout->n);
      }
    } else {
      printf("onego failed\n");
    }
    ti->rcu_stop();
  }
  return 0;
}

static String log_filename(const char* logdir, int logindex) {
    struct stat sb;
    int r = stat(logdir, &sb);
    if (r < 0 && errno == ENOENT) {
	r = mkdir(logdir, 0777);
	if (r < 0) {
	    fprintf(stderr, "%s: %s\n", logdir, strerror(errno));
	    mandatory_assert(0);
	}
    }

    StringAccum sa;
    sa.snprintf(strlen(logdir) + 24, "%s/kvd-log-%d", logdir, logindex);
    return sa.take_string();
}

void log_init() {
  int ret, i;

  logs = logset::make(nlogger);
  for (i = 0; i < nlogger; i++)
      logs->log(i).initialize(log_filename(logdir[i % nlogdir], i));

  cks = (ckstate *)malloc(sizeof(ckstate) * nckthreads);
  for (i = 0; i < nckthreads; i++) {
    threadinfo *ti = threadinfo::make(threadinfo::TI_CHECKPOINT, i);
    cks[i].state = CKState_Uninit;
    cks[i].ti = ti;
    ret = pthread_create(&ti->ti_threadid, 0, conc_checkpointer, ti);
    mandatory_assert(ret == 0);
  }
}

// p points to key\0val\0
void insert_from_checkpoint(char *p, threadinfo *ti) {
    int keylen = strlen(p);
    mandatory_assert(keylen >= 0 && keylen < MaxKeyLen);
    kvtimestamp_t ts = *(kvtimestamp_t*)(p + keylen + 1);
    int vlen = *(int*)(p + keylen + 1 + sizeof(ts));
    Str key(p, keylen);
    Str value(p + keylen + 1 + sizeof(ts) + sizeof(vlen), vlen);
    tree->checkpoint_restore(key, value, ts, ti);
}

// read a checkpoint, insert key/value pairs into tree.
// must be followed by a read of the log!
// since checkpoint is not consistent
// with any one point in time.
// returns the timestamp of the first log record that needs
// to come from the log.
kvepoch_t read_checkpoint(threadinfo *ti, const char *path) {
  double t0 = now();

  int fd = open(path, 0);
  if(fd < 0){
    printf("no %s\n", path);
    return 0;
  }
  struct stat sb;
  int ret = fstat(fd, &sb);
  mandatory_assert(ret == 0);
  char *p = (char *) mmap(0, sb.st_size, PROT_READ, MAP_FILE|MAP_PRIVATE, fd, 0);
  mandatory_assert(p != MAP_FAILED);
  close(fd);

  uint64_t gen = *(uint64_t *)p;
  uint64_t n = *(uint64_t *)(p + 8);
  printf("reading checkpoint with %" PRIu64 " nodes\n", n);

  char *keyjunk = p + 2*8; // array of key prefixes, ignore for now
  uint64_t *ind = (uint64_t *)(keyjunk + CkpKeyPrefixLen*n);
  char *keyval = (char *)(ind + n);
  query<row_type> q;

  // round n up to power of two
  uint64_t n2 = pow(2, ceil(log(n) / log(2)));
  mandatory_assert(n2 >= n);
  traverse_checkpoint_inorder(0, n2, keyval, ind, n, ti);
  munmap(p, sb.st_size);

  double t1 = now();

  printf("%.1f MB, %.2f sec, %.1f MB/sec\n",
         sb.st_size / 1000000.0,
         t1 - t0,
         (sb.st_size / 1000000.0) / (t1 - t0));

  return gen;
}

void
waituntilphase(int phase)
{
  mandatory_assert(pthread_mutex_lock(&rec_mu) == 0);
  while (rec_state != phase)
    mandatory_assert(pthread_cond_wait(&rec_cond, &rec_mu) == 0);
  mandatory_assert(pthread_mutex_unlock(&rec_mu) == 0);
}

void
inactive(void)
{
  mandatory_assert(pthread_mutex_lock(&rec_mu) == 0);
  rec_nactive --;
  mandatory_assert(pthread_cond_broadcast(&rec_cond) == 0);
  mandatory_assert(pthread_mutex_unlock(&rec_mu) == 0);
}

uint64_t
traverse_checkpoint_inorder(uint64_t off, uint64_t n,
                            char *base, uint64_t *ind, uint64_t max,
                            threadinfo *ti)
{
    mandatory_assert(off == 0);
    for (uint64_t i = 0; i < max; i++)
        insert_from_checkpoint(base + ind[i], ti);
    return n;
}

void recovercheckpoint(threadinfo *ti) {
    waituntilphase(REC_CKP);
    char path[256];
    sprintf(path, "%s/kvd-ckp-%" PRId64 "-%d", ckpdir[ti->ti_index % nckpdir],
            ckp_gen.value(), ti->ti_index);
    kvepoch_t gen = read_checkpoint(ti, path);
    mandatory_assert(ckp_gen == gen);
    inactive();
}

void
recphase(int nactive, int state)
{
  rec_nactive = nactive;
  rec_state = state;
  mandatory_assert(pthread_cond_broadcast(&rec_cond) == 0);
  while (rec_nactive)
    mandatory_assert(pthread_cond_wait(&rec_cond, &rec_mu) == 0);
}

// read the checkpoint file.
// read each log file.
// insert will ignore attempts to update with timestamps
// less than what was in the entry from the checkpoint file.
// so we don't have to do an explicit merge by time of the log files.
void
recover(threadinfo *)
{
  recovering = true;
  // XXX: discard temporary checkpoint and ckp-gen files generated before crash

  // get the generation of the checkpoint from ckp-gen, if any
  char path[256];
  sprintf(path, "%s/kvd-ckp-gen", ckpdir[0]);
  ckp_gen = 0;
  rec_ckp_min_epoch = rec_ckp_max_epoch = 0;
  int fd = open(path, O_RDONLY);
  if (fd >= 0) {
      Json ckpj = Json::parse(read_file_contents(fd));
      close(fd);
      if (ckpj && ckpj["kvdb_checkpoint"] && ckpj["generation"].is_number()) {
	  ckp_gen = ckpj["generation"].to_u64();
	  rec_ckp_min_epoch = ckpj["min_epoch"].to_u64();
	  rec_ckp_max_epoch = ckpj["max_epoch"].to_u64();
	  printf("recover from checkpoint %" PRIu64 " [%" PRIu64 ", %" PRIu64 "]\n", ckp_gen.value(), rec_ckp_min_epoch.value(), rec_ckp_max_epoch.value());
      }
  } else {
    printf("no %s\n", path);
  }
  mandatory_assert(pthread_mutex_lock(&rec_mu) == 0);

  // recover from checkpoint, and set timestamp of the checkpoint
  recphase(nckthreads, REC_CKP);

  // find minimum maximum timestamp of entries in each log
  rec_log_infos = new logreplay::info_type[nlogger];
  recphase(nlogger, REC_LOG_TS);

  // replay log entries, remove inconsistent entries, and append
  // an empty log entry with minimum timestamp

  // calculate log range

  // Maximum epoch seen in the union of the logs and the checkpoint. (We
  // don't commit a checkpoint until all logs are flushed past the
  // checkpoint's max_epoch.)
  kvepoch_t max_epoch = rec_ckp_max_epoch;
  if (max_epoch)
      max_epoch = max_epoch.next_nonzero();
  for (logreplay::info_type *it = rec_log_infos;
       it != rec_log_infos + nlogger; ++it)
      if (it->last_epoch
	  && (!max_epoch || max_epoch < it->last_epoch))
	  max_epoch = it->last_epoch;

  // Maximum first_epoch seen in the logs. Full log information is not
  // available for epochs before max_first_epoch.
  kvepoch_t max_first_epoch = 0;
  for (logreplay::info_type *it = rec_log_infos;
       it != rec_log_infos + nlogger; ++it)
      if (it->first_epoch
	  && (!max_first_epoch || max_first_epoch < it->first_epoch))
	  max_first_epoch = it->first_epoch;

  // Maximum epoch of all logged wake commands.
  kvepoch_t max_wake_epoch = 0;
  for (logreplay::info_type *it = rec_log_infos;
       it != rec_log_infos + nlogger; ++it)
      if (it->wake_epoch
	  && (!max_wake_epoch || max_wake_epoch < it->wake_epoch))
	  max_wake_epoch = it->wake_epoch;

  // Minimum last_epoch seen in QUIESCENT logs.
  kvepoch_t min_quiescent_last_epoch = 0;
  for (logreplay::info_type *it = rec_log_infos;
       it != rec_log_infos + nlogger; ++it)
      if (it->quiescent
	  && (!min_quiescent_last_epoch || min_quiescent_last_epoch > it->last_epoch))
	  min_quiescent_last_epoch = it->last_epoch;

  // If max_wake_epoch && min_quiescent_last_epoch <= max_wake_epoch, then a
  // wake command was missed by at least one quiescent log. We can't replay
  // anything at or beyond the minimum missed wake epoch. So record, for
  // each log, the minimum wake command that at least one quiescent thread
  // missed.
  if (max_wake_epoch && min_quiescent_last_epoch <= max_wake_epoch)
      rec_replay_min_quiescent_last_epoch = min_quiescent_last_epoch;
  else
      rec_replay_min_quiescent_last_epoch = 0;
  recphase(nlogger, REC_LOG_ANALYZE_WAKE);

  // Calculate upper bound of epochs to replay.
  // This is the minimum of min_post_quiescent_wake_epoch (if any) and the
  // last_epoch of all non-quiescent logs.
  rec_replay_max_epoch = max_epoch;
  for (logreplay::info_type *it = rec_log_infos;
       it != rec_log_infos + nlogger; ++it) {
      if (!it->quiescent
          && it->last_epoch
	  && it->last_epoch < rec_replay_max_epoch)
	  rec_replay_max_epoch = it->last_epoch;
      if (it->min_post_quiescent_wake_epoch
          && it->min_post_quiescent_wake_epoch < rec_replay_max_epoch)
          rec_replay_max_epoch = it->min_post_quiescent_wake_epoch;
  }

  // Calculate lower bound of epochs to replay.
  rec_replay_min_epoch = rec_ckp_min_epoch;
  // XXX what about max_first_epoch?

  // Checks.
  if (rec_ckp_min_epoch) {
      mandatory_assert(rec_ckp_min_epoch > max_first_epoch);
      mandatory_assert(rec_ckp_min_epoch < rec_replay_max_epoch);
      mandatory_assert(rec_ckp_max_epoch < rec_replay_max_epoch);
      fprintf(stderr, "replay [%" PRIu64 ",%" PRIu64 ") from [%" PRIu64 ",%" PRIu64 ") into ckp [%" PRIu64 ",%" PRIu64 "]\n",
	      rec_replay_min_epoch.value(), rec_replay_max_epoch.value(),
	      max_first_epoch.value(), max_epoch.value(),
	      rec_ckp_min_epoch.value(), rec_ckp_max_epoch.value());
  }

  // Actually replay.
  delete[] rec_log_infos;
  rec_log_infos = 0;
  recphase(nlogger, REC_LOG_REPLAY);

  // done recovering
  recphase(0, REC_DONE);
#if !NDEBUG
  // check that all delta markers have been recycled (leaving only remove
  // markers and real values)
  uint64_t deltas_created = 0, deltas_removed = 0;
  for (threadinfo *ti = threadinfo::allthreads; ti; ti = ti->ti_next) {
      deltas_created += ti->pstat.deltas_created;
      deltas_removed += ti->pstat.deltas_removed;
  }
  if (deltas_created)
      fprintf(stderr, "deltas created: %" PRIu64 ", removed: %" PRIu64 "\n", deltas_created, deltas_removed);
  mandatory_assert(deltas_created == deltas_removed);
#endif

  global_log_epoch = rec_replay_max_epoch.next_nonzero();

  mandatory_assert(pthread_mutex_unlock(&rec_mu) == 0);
  recovering = false;
  if (recovery_only)
      exit(0);
}

void
writecheckpoint(const char *path, ckstate *c, double t0)
{
  double t1 = now();
  printf("memory phase: %" PRIu64 " nodes, %.2f sec\n", c->count, t1 - t0);

  int fd = creat(path, 0666);
  mandatory_assert(fd >= 0);

  // checkpoint file format:
  //   ckp_gen (64 bits)
  //   #keys (64 bits)
  //   #keys * CKKEYLEN key prefixes, in lexical order
  //   #keys * CKKEYLEN 64-bit indices, in lexical order, into...
  //   key/val pairs

  checked_write(fd, &ckp_gen, sizeof(ckp_gen));
  checked_write(fd, &c->count, sizeof(c->count));
  checked_write(fd, c->keys->buf, c->keys->n);
  checked_write(fd, c->ind->buf, c->ind->n);
  checked_write(fd, c->vals->buf, c->vals->n);

  int ret = fsync(fd);
  mandatory_assert(ret == 0);
  ret = close(fd);
  mandatory_assert(ret == 0);

  double t2 = now();
  c->bytes = c->keys->n + c->ind->n + c->vals->n;
  printf("file phase (%s): %" PRIu64 " bytes, %.2f sec, %.1f MB/sec\n",
         path,
         c->bytes,
         t2 - t1,
         (c->bytes / 1000000.0) / (t2 - t1));
}

void
conc_filecheckpoint(threadinfo *ti)
{
  ckstate *c = &cks[ti->ti_index];
  c->keys = new_bufkvout();
  c->vals = new_bufkvout();
  c->ind = new_bufkvout();
  double t0 = now();
  tree->scan(c->q, ti);
  char path[256];
  sprintf(path, "%s/kvd-ckp-%" PRId64 "-%d", ckpdir[ti->ti_index % nckpdir],
          ckp_gen.value(), ti->ti_index);
  writecheckpoint(path, c, t0);
  c->count = 0;
  free(c->keys);
  free(c->vals);
  free(c->ind);
}

static Json
prepare_checkpoint(kvepoch_t min_epoch, int nckthreads, const Str *pv)
{
    Json j;
    j.set("kvdb_checkpoint", true)
	.set("min_epoch", min_epoch.value())
	.set("max_epoch", global_log_epoch.value())
	.set("generation", ckp_gen.value())
	.set("nckthreads", nckthreads);

    Json pvj;
    for (int i = 1; i < nckthreads; ++i)
	pvj.push_back(Json::make_string(pv[i].s, pv[i].len));
    j.set("pivots", pvj);

    return j;
}

static void
commit_checkpoint(Json ckpj)
{
    // atomically commit a set of checkpoint files by incrementing
    // the checkpoint generation on disk
    char path[256];
    sprintf(path, "%s/kvd-ckp-gen", ckpdir[0]);
    int r = atomic_write_file_contents(path, ckpj.unparse());
    mandatory_assert(r == 0);
    fprintf(stderr, "kvd-ckp-%" PRIu64 " [%s,%s]: committed\n",
	    ckp_gen.value(), ckpj["min_epoch"].to_s().c_str(),
	    ckpj["max_epoch"].to_s().c_str());

    // delete old checkpoint files
    for (int i = 0; i < nckthreads; i++) {
	char path[256];
	sprintf(path, "%s/kvd-ckp-%" PRId64 "-%d", ckpdir[i % nckpdir],
		ckp_gen.value() - 1, i);
	unlink(path);
    }
}

static kvepoch_t
max_flushed_epoch()
{
    kvepoch_t mfe = 0, ge = global_log_epoch;
    for (int i = 0; i < nlogger; ++i) {
        loginfo& log = logs->log(i);
	kvepoch_t fe = log.quiescent() ? ge : log.flushed_epoch();
	if (!mfe || fe < mfe)
	    mfe = fe;
    }
    return mfe;
}

// concurrent periodic checkpoint
void *
conc_checkpointer(void *xarg)
{
  threadinfo *ti = (threadinfo *) xarg;
  ti->enter();
  recovercheckpoint(ti);
  ckstate *c = &cks[ti->ti_index];
  c->count = 0;
  pthread_cond_init(&c->state_cond, NULL);
  c->state = CKState_Ready;
  while (recovering)
    sleep(1);
  if (checkpoint_interval <= 0)
      return 0;
  if (ti->ti_index == 0) {
    for (int i = 1; i < nckthreads; i++)
      while (cks[i].state != CKState_Ready)
        ;
    Str *pv = new Str[nckthreads + 1];
    Json uncommitted_ckp;

    while (1) {
      struct timespec ts;
      set_timespec(ts, now() + (uncommitted_ckp ? 0.25 : checkpoint_interval));

      pthread_mutex_lock(&checkpoint_mu);
      if (!go_quit)
        pthread_cond_timedwait(&checkpoint_cond, &checkpoint_mu, &ts);
      if (go_quit) {
          for (int i = 0; i < nckthreads; i++) {
              cks[i].state = CKState_Quit;
              pthread_cond_signal(&cks[i].state_cond);
          }
          pthread_mutex_unlock(&checkpoint_mu);
          break;
      }
      pthread_mutex_unlock(&checkpoint_mu);

      if (uncommitted_ckp) {
	  kvepoch_t mfe = max_flushed_epoch();
	  if (!mfe || mfe > uncommitted_ckp["max_epoch"].to_u64()) {
	      commit_checkpoint(uncommitted_ckp);
	      uncommitted_ckp = Json();
	  }
	  continue;
      }

      double t0 = now();
      ti->rcu_start();
      for (int i = 0; i < nckthreads + 1; i++)
        pv[i].assign(NULL, 0);
      tree->findpivots(pv, nckthreads + 1);
      ti->rcu_stop();

      kvepoch_t min_epoch = global_log_epoch;
      pthread_mutex_lock(&checkpoint_mu);
      ckp_gen = ckp_gen.next_nonzero();
      for (int i = 0; i < nckthreads; i++) {
	  Str endkey = (i == nckthreads - 1 ? Str() : pv[i + 1]);
	  cks[i].q.begin_checkpoint(&cks[i], pv[i], endkey);
	  cks[i].state = CKState_Go;
	  pthread_cond_signal(&cks[i].state_cond);
      }
      pthread_mutex_unlock(&checkpoint_mu);

      ti->rcu_start();
      conc_filecheckpoint(ti);
      ti->rcu_stop();

      cks[0].state = CKState_Ready;
      uint64_t bytes = cks[0].bytes;
      pthread_mutex_lock(&checkpoint_mu);
      for (int i = 1; i < nckthreads; i++) {
        while (cks[i].state != CKState_Ready)
          pthread_cond_wait(&cks[i].state_cond, &checkpoint_mu);
        bytes += cks[i].bytes;
      }
      pthread_mutex_unlock(&checkpoint_mu);

      uncommitted_ckp = prepare_checkpoint(min_epoch, nckthreads, pv);

      for (int i = 0; i < nckthreads + 1; i++)
        if (pv[i].s)
          free((void *)pv[i].s);
      double t = now() - t0;
      fprintf(stderr, "kvd-ckp-%" PRIu64 " [%s,%s]: prepared (%.2f sec, %" PRIu64 " MB, %" PRIu64 " MB/sec)\n",
	      ckp_gen.value(), uncommitted_ckp["min_epoch"].to_s().c_str(),
	      uncommitted_ckp["max_epoch"].to_s().c_str(),
	      t, bytes / (1 << 20), (uint64_t)(bytes / t) >> 20);
    }
  } else {
    while(1) {
      pthread_mutex_lock(&checkpoint_mu);
      while (c->state != CKState_Go && c->state != CKState_Quit)
        pthread_cond_wait(&c->state_cond, &checkpoint_mu);
      if (c->state == CKState_Quit) {
        pthread_mutex_unlock(&checkpoint_mu);
        break;
      }
      pthread_mutex_unlock(&checkpoint_mu);

      ti->rcu_start();
      conc_filecheckpoint(ti);
      ti->rcu_stop();

      pthread_mutex_lock(&checkpoint_mu);
      c->state = CKState_Ready;
      pthread_cond_signal(&c->state_cond);
      pthread_mutex_unlock(&checkpoint_mu);
    }
  }
  return 0;
}
