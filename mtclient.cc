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
//
// kvc: key/value client
//
// cc -O -o kvc kvc.c kvio.c
//

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include <limits.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/wait.h>
#include <assert.h>
#include <string.h>
#include <pthread.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/select.h>
#include <arpa/inet.h>
#include <math.h>
#include <fcntl.h>
#include "kvstats.hh"
#include "kvio.hh"
#include "json.hh"
#include "kvtest.hh"
#include "mtclient.hh"
#include "kvrandom.hh"
#include "clp.h"

const char *serverip = "127.0.0.1";

typedef void (*get_async_cb)(struct child *c, struct async *a,
			     bool has_val, const Str &val);
typedef void (*put_async_cb)(struct child *c, struct async *a,
			     int status);
typedef void (*remove_async_cb)(struct child *c, struct async *a,
				int status);

struct async {
    int cmd; // Cmd_ constant
    unsigned seq;
    union {
	get_async_cb get_fn;
	put_async_cb put_fn;
	remove_async_cb remove_fn;
    };
    char key[16]; // just first 16 bytes
    char wanted[16]; // just first 16 bytes
    int wantedlen;
    int acked;
};
#define MAXWINDOW 500
int window = MAXWINDOW;

#define SEQMOD 10000000

struct child {
  int s;
  int udp; // 1 -> udp, 0 -> tcp
  KVConn *conn;

  struct async a[MAXWINDOW];
  long long nw; // # sent
  long long nr; // # we're seen replies for
  int childno;
};
void aget(struct child *, const Str &key, const Str &wanted, get_async_cb fn);
void ascan(struct child *c, int numpairs, const char *key);
void child(void (*fn)(struct child *), int childno);
void checkasync(struct child *c, int force);

void sync_rw1(struct child *);
void rw1(struct child *);
void udp1(struct child *);
void rw2(struct child *);
void rw3(struct child *);
void rw4(struct child *);
void rw5(struct child *);
void rw16(struct child *);
void r1(struct child *);
void w1(struct child *);
void w1b(struct child *);
void wd1(struct child *);
void wd1m1(struct child *);
void wd1m2(struct child *);
void wd1check(struct child *);
void wd1m1check(struct child *);
void wd1m2check(struct child *);
void wd2(struct child *);
void wd2check(struct child *);
void tri1(struct child *);
void tri1check(struct child *);
void u1(struct child *);
void same(struct child *);
void over1(struct child *);
void over2(struct child *);
void rec1(struct child *);
void rec2(struct child *);
void cpa(struct child *);
void cpb(struct child *);
void cpc(struct child *);
void cpd(struct child *);
void volt1a(struct child *);
void volt1b(struct child *);
void volt2a(struct child *);
void volt2b(struct child *);
void scantest(struct child *);
void wscale(struct child *);
void ruscale_init(struct child *);
void rscale(struct child *);
void uscale(struct child *);
void long_go(struct child *);
void long_init(struct child *);

static int children = 1;
static uint64_t nkeys = 0;
static int prefixLen = 0;
static int keylen = 0;
static uint64_t limit = ~uint64_t(0);
double duration = 10;
double duration2 = 0;
int udpflag = 0;
int quiet = 0;
int first_server_port = 2117;
// Should all child processes connects to the same UDP PORT on server
bool share_server_port = false;
volatile bool timeout[2] = {false, false};
int first_local_port = 0;
const char *input = NULL;
static int rsinit_part = 0;
static int first_seed = 0;
static int rscale_partsz = 0;
static int getratio = -1;
static int minkeyletter = '0';
static int maxkeyletter = '9';

static struct {
  const char *name;
  void (*fn)(struct child *);
} tests[] = {
  { "sync_rw1", sync_rw1 },
  { "rw1", rw1 },
  { "udp1", udp1 },
  { "rw2", rw2 },
  { "rw3", rw3 },
  { "rw4", rw4 },
  { "rw5", rw5},
  { "rw16", rw16 },
  { "r1", r1 },
  { "w1", w1 },
  { "w1b", w1b },
  { "u1", u1 },
  { "wd1", wd1 },
  { "wd1m1", wd1m1 },
  { "wd1m2", wd1m2 },
  { "wd1check", wd1check },
  { "wd1m1check", wd1m1check },
  { "wd1m2check", wd1m2check },
  { "wd2", wd2 },
  { "wd2check", wd2check },
  { "tri1", tri1 },
  { "tri1check", tri1check },
  { "same", same },
  { "over1", over1 },
  { "over2", over2 },
  { "rec1", rec1 },
  { "rec2", rec2 },
  { "cpa", cpa },
  { "cpb", cpb },
  { "cpc", cpc },
  { "cpd", cpd },
  { "volt1a", volt1a },
  { "volt1b", volt1b },
  { "volt2a", volt2a },
  { "volt2b", volt2b },
  { "scantest", scantest },
  { "wscale", wscale},
  { "ruscale_init", ruscale_init},
  { "rscale", rscale},
  { "uscale", uscale},
  { "long_init", long_init},
  { "long_go", long_go},
  { 0, 0 }
};


void
usage()
{
  fprintf(stderr, "Usage: kvc [-s serverip] [-w window] [--udp] "\
	  "[-j nchildren] [-d duration] [--ssp] [--flp first_local_port] "\
	  "[--fsp first_server_port] [-i json_input]");
  int i;
  for(i = 0; tests[i].name; i++)
    fprintf(stderr, "%s|", tests[i].name);
  fprintf(stderr, "\n");
  exit(1);
}

void
settimeout(int)
{
  if (!timeout[0]) {
    timeout[0] = true;
    if (duration2)
	alarm((int) ceil(duration2));
  } else
    timeout[1] = true;
}

enum { clp_val_suffixdouble = Clp_ValFirstUser };
enum { opt_threads = 1, opt_threads_deprecated, opt_duration, opt_duration2,
       opt_window, opt_server, opt_first_server_port, opt_quiet, opt_udp,
       opt_first_local_port, opt_share_server_port, opt_input,
       opt_rsinit_part, opt_first_seed, opt_rscale_partsz, opt_keylen,
       opt_limit, opt_prefix_len, opt_nkeys, opt_get_ratio, opt_minkeyletter,
       opt_maxkeyletter, opt_nofork };
static const Clp_Option options[] = {
    { "threads", 'j', opt_threads, Clp_ValInt, 0 },
    { 0, 'n', opt_threads_deprecated, Clp_ValInt, 0 },
    { "duration", 'd', opt_duration, Clp_ValDouble, 0 },
    { "duration2", 0, opt_duration2, Clp_ValDouble, 0 },
    { "d2", 0, opt_duration2, Clp_ValDouble, 0 },
    { "window", 'w', opt_window, Clp_ValInt, 0 },
    { "server-ip", 's', opt_server, Clp_ValString, 0 },
    { "first-server-port", 0, opt_first_server_port, Clp_ValInt, 0 },
    { "fsp", 0, opt_first_server_port, Clp_ValInt, 0 },
    { "quiet", 'q', opt_quiet, 0, Clp_Negate },
    { "udp", 'u', opt_udp, 0, Clp_Negate },
    { "first-local-port", 0, opt_first_local_port, Clp_ValInt, 0 },
    { "flp", 0, opt_first_local_port, Clp_ValInt, 0 },
    { "share-server-port", 0, opt_share_server_port, 0, Clp_Negate },
    { "ssp", 0, opt_share_server_port, 0, Clp_Negate },
    { "input", 'i', opt_input, Clp_ValString, 0 },
    { "rsinit_part", 0, opt_rsinit_part, Clp_ValInt, 0 },
    { "first_seed", 0, opt_first_seed, Clp_ValInt, 0 },
    { "rscale_partsz", 0, opt_rscale_partsz, Clp_ValInt, 0 },
    { "keylen", 0, opt_keylen, Clp_ValInt, 0 },
    { "limit", 'l', opt_limit, clp_val_suffixdouble, 0 },
    { "prefixLen", 0, opt_prefix_len, Clp_ValInt, 0 },
    { "nkeys", 0, opt_nkeys, Clp_ValInt, 0 },
    { "getratio", 0, opt_get_ratio, Clp_ValInt, 0 },
    { "minkeyletter", 0, opt_minkeyletter, Clp_ValString, 0 },
    { "maxkeyletter", 0, opt_maxkeyletter, Clp_ValString, 0 },
    { "no-fork", 0, opt_nofork, 0, 0 }
};

int
main(int argc, char *argv[])
{
  int i, pid, status;
  int test = 0;
  int pipes[512];
  int dofork = 1;

  Clp_Parser *clp = Clp_NewParser(argc, argv, (int) arraysize(options), options);
  Clp_AddType(clp, clp_val_suffixdouble, Clp_DisallowOptions, clp_parse_suffixdouble, 0);
  int opt;
  while ((opt = Clp_Next(clp)) >= 0) {
      switch (opt) {
      case opt_threads:
	  children = clp->val.i;
	  break;
      case opt_threads_deprecated:
	  Clp_OptionError(clp, "%<%O%> is deprecated, use %<-j%>");
	  children = clp->val.i;
	  break;
      case opt_duration:
	  duration = clp->val.d;
	  break;
      case opt_duration2:
	  duration2 = clp->val.d;
	  break;
      case opt_window:
	  window = clp->val.i;
	  break;
      case opt_server:
	  serverip = clp->vstr;
	  break;
      case opt_first_server_port:
	  first_server_port = clp->val.i;
	  break;
      case opt_quiet:
	  quiet = !clp->negated;
	  break;
      case opt_udp:
	  udpflag = !clp->negated;
	  break;
      case opt_first_local_port:
	  first_local_port = clp->val.i;
	  break;
      case opt_share_server_port:
	  share_server_port = !clp->negated;
	  break;
      case opt_input:
	  input = clp->vstr;
	  break;
      case opt_rsinit_part:
	  rsinit_part = clp->val.i;
	  break;
      case opt_first_seed:
	  first_seed = clp->val.i;
	  break;
      case opt_rscale_partsz:
	  rscale_partsz = clp->val.i;
	  break;
      case opt_keylen:
	  keylen = clp->val.i;
	  break;
      case opt_limit:
	  limit = (uint64_t) clp->val.d;
	  break;
      case opt_prefix_len:
	  prefixLen = clp->val.i;
	  break;
      case opt_nkeys:
	  nkeys = clp->val.i;
	  break;
      case opt_get_ratio:
	  getratio = clp->val.i;
	  break;
      case opt_minkeyletter:
	  minkeyletter = clp->vstr[0];
	  break;
      case opt_maxkeyletter:
	  maxkeyletter = clp->vstr[0];
	  break;
      case opt_nofork:
	  dofork = !clp->negated;
	  break;
      case Clp_NotOption: {
	  int j;
	  for (j = 0; tests[j].name; j++)
	      if (strcmp(clp->vstr, tests[j].name) == 0)
		  break;
	  if (tests[j].name == 0)
	      usage();
	  test = j;
      }
      }
  }
  if(children < 1 || (children != 1 && !dofork))
    usage();

  printf("%s, w %d, test %s, children %d\n",
         udpflag ? "udp" : "tcp", window,
         tests[test].name, children);

  if (dofork)  {
      for(i = 0; i < children; i++){
	  int ptmp[2];
	  int r = pipe(ptmp);
	  mandatory_assert(r == 0);
	  fflush(stdout);
	  pid = fork();
	  if(pid < 0){
	      perror("fork");
	      exit(1);
	  }
	  if(pid == 0){
	      close(ptmp[0]);
	      dup2(ptmp[1], 1);
	      close(ptmp[1]);
	      signal(SIGALRM, settimeout);
	      alarm((int) ceil(duration));
	      child(tests[test].fn, i);
	      exit(0);
	  }
	  pipes[i] = ptmp[0];
	  close(ptmp[1]);
      }
      for(i = 0; i < children; i++){
	  if(wait(&status) <= 0){
	      perror("wait");
	      exit(1);
	  }
	  if (WIFSIGNALED(status))
	      fprintf(stderr, "child %d died by signal %d\n", i, WTERMSIG(status));
      }
  } else
      child(tests[test].fn, 0);

  long long total = 0;
  kvstats puts, gets, scans, puts_per_sec, gets_per_sec, scans_per_sec;
  for(i = 0; i < children; i++){
    char buf[2048];
    int cc = read(pipes[i], buf, sizeof(buf)-1);
    assert(cc > 0);
    buf[cc] = 0;
    printf("%s", buf);
    Json bufj = Json::parse(buf, buf + cc);
    long long iv;
    double dv;
    if (bufj.to_i(iv))
	total += iv;
    else if (bufj.is_object()) {
	if (bufj.get("ops", iv)
	    || bufj.get("total", iv)
	    || bufj.get("count", iv))
	    total += iv;
	if (bufj.get("puts", iv))
	    puts.add(iv);
	if (bufj.get("gets", iv))
	    gets.add(iv);
	if (bufj.get("scans", iv))
	    scans.add(iv);
	if (bufj.get("puts_per_sec", dv))
	    puts_per_sec.add(dv);
	if (bufj.get("gets_per_sec", dv))
	    gets_per_sec.add(dv);
	if (bufj.get("scans_per_sec", dv))
	    scans_per_sec.add(dv);
    }
  }

  printf("total %lld\n", total);
  puts.print_report("puts");
  gets.print_report("gets");
  scans.print_report("scans");
  puts_per_sec.print_report("puts/s");
  gets_per_sec.print_report("gets/s");
  scans_per_sec.print_report("scans/s");

  exit(0);
}

void
child(void (*fn)(struct child *), int childno)
{
  struct sockaddr_in sin;
  int ret, yes = 1;
  struct child c;

  bzero(&c, sizeof(c));
  c.childno = childno;

  if(udpflag){
    c.udp = 1;
    c.s = socket(AF_INET, SOCK_DGRAM, 0);
  } else {
    c.s = socket(AF_INET, SOCK_STREAM, 0);
  }
  if (first_local_port) {
    bzero(&sin, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_port = htons(first_local_port + (childno % 48));
    ret = ::bind(c.s, (struct sockaddr *) &sin, sizeof(sin));
    if (ret < 0) {
      perror("bind");
      exit(1);
    }
  }

  assert(c.s >= 0);
  setsockopt(c.s, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

  bzero(&sin, sizeof(sin));
  sin.sin_family = AF_INET;
  if (udpflag && !share_server_port)
    sin.sin_port = htons(first_server_port + (childno % 48));
  else
    sin.sin_port = htons(first_server_port);
  sin.sin_addr.s_addr = inet_addr(serverip);
  ret = connect(c.s, (struct sockaddr *) &sin, sizeof(sin));
  if(ret < 0){
    perror("connect");
    exit(1);
  }

  c.conn = new KVConn(c.s, !udpflag);

  (*fn)(&c);

  checkasync(&c, 2);

  delete c.conn;
  close(c.s);
}

int
get(struct child *c, const Str &key, char *val, int max)
{
  assert(c->nr == c->nw);

  unsigned int sseq = c->nw % SEQMOD;
  c->nw++;
  c->nr++;

  c->conn->sendgetwhole(key, sseq);
  c->conn->flush();

  unsigned int rseq = 0;
  vector<string> row;
  int r = c->conn->recvget(row, &rseq, false);
  mandatory_assert(r == 0);
  mandatory_assert(rseq == sseq);
  if (row.size() == 0)
    return -1;
  mandatory_assert(row.size() == 1 && row[0].length() <= (unsigned)max);
  memcpy(val, row[0].data(), row[0].length());
  return row[0].length();
}

// builtin aget callback: no check
void
nocheck(struct child *, struct async *, bool, const Str &)
{
}

// builtin aget callback: store string
void
asyncgetcb(struct child *, struct async *a, bool, const Str &val)
{
    Str *sptr;
    assert(a->wantedlen == sizeof(Str *));
    memcpy(&sptr, a->wanted, sizeof(Str *));
    sptr->len = std::min(sptr->len, val.len);
    memcpy(const_cast<char *>(sptr->s), val.s, sptr->len);
}

// builtin aget callback: store string
void
asyncgetcb_int(struct child *, struct async *a, bool, const Str &val)
{
    int *vptr;
    assert(a->wantedlen == sizeof(int *));
    memcpy(&vptr, a->wanted, sizeof(int *));
    long x = 0;
    if (val.len <= 0)
	x = -1;
    else
	for (int i = 0; i < val.len; ++i)
	    if (val.s[i] >= '0' && val.s[i] <= '9')
		x = (x * 10) + (val.s[i] - '0');
	    else {
		x = -1;
		break;
	    }
    *vptr = x;
}

// default aget callback: check val against wanted
void
defaultget(struct child *, struct async *a, bool have_val, const Str &val)
{
    // check that we got the expected value
    int wanted_avail = std::min(a->wantedlen, int(sizeof(a->wanted)));
    if (!have_val
	|| a->wantedlen != val.len
	|| memcmp(val.s, a->wanted, wanted_avail) != 0)
	fprintf(stderr, "oops wanted %.*s(%d) got %.*s(%d)\n",
		wanted_avail, a->wanted, a->wantedlen, val.len, val.s, val.len);
    else {
	mandatory_assert(a->wantedlen == val.len);
	mandatory_assert(memcmp(val.s, a->wanted, wanted_avail) == 0);
    }
}

// builtin aput/aremove callback: store status
void
asyncputcb(struct child *, struct async *a, int status)
{
    int *sptr;
    assert(a->wantedlen == sizeof(int *));
    memcpy(&sptr, a->wanted, sizeof(int *));
    *sptr = status;
}

// process any waiting replies to aget() and aput().
// force=0 means non-blocking check if anything waiting on socket.
// force=1 means wait for at least one reply.
// force=2 means wait for all pending (nw-nr) replies.
void
checkasync(struct child *c, int force)
{
  while(c->nw > c->nr){
    if(force)
      c->conn->flush();
    if(kvcheck(c->conn->in, force ? 2 : 1) > 0){
      unsigned int rseq = 0;
      c->conn->readseq(&rseq);

      // is rseq in the nr..nw window?
      // replies might arrive out of order if UDP
      int nn;
      for(nn = c->nr; nn < c->nw; nn++)
        if((int) rseq == (nn % SEQMOD))
          break;
      if(nn >= c->nw)
        fprintf(stderr, "rseq %d nr %qd nw %qd\n", rseq, c->nr, c->nw);
      mandatory_assert(nn < c->nw);
      struct async *a = &c->a[nn % window];
      mandatory_assert(a->seq == rseq);

      // advance the nr..nw window
      mandatory_assert(a->acked == 0);
      a->acked = 1;
      while(c->nr < c->nw && c->a[c->nr % window].acked)
        c->nr += 1;

      // might have been the last free slot,
      // don't want to re-use it underfoot.
      struct async tmpa = *a;

      if(tmpa.cmd == Cmd_Get){
        // this is a reply to a get
        vector<string> row;
	int r = c->conn->recvget(row, NULL, true);
	mandatory_assert(r == 0);
	Str s = (row.size() ? Str(row[0].data(), row[0].length()) : Str());
	if (tmpa.get_fn)
	    (tmpa.get_fn)(c, &tmpa, row.size(), s);
      } else if(tmpa.cmd == Cmd_Put){
	  // this is a reply to a put
	  int r = c->conn->recvput(NULL, true);
	  mandatory_assert(r >= 0);
      } else if(tmpa.cmd == Cmd_Put_Status){
	  // this is a reply to a put
	  int status;
	  int r = c->conn->recvputstatus(&status, NULL, true);
	  mandatory_assert(r >= 0);
	  if (tmpa.put_fn)
	      (tmpa.put_fn)(c, &tmpa, status);
      } else if(tmpa.cmd == Cmd_Scan){
        // this is a reply to a scan
        vector<string> keys;
        vector< vector<string> > rows;
	int r = c->conn->recvscan(keys, rows, NULL, true);
	mandatory_assert(r == 0);
        mandatory_assert(keys.size() <= (unsigned)tmpa.wantedlen);
      } else if (tmpa.cmd == Cmd_Remove) {
	  // this is a reply to a remove
	  int status;
	  int r = c->conn->recvremove(&status, NULL, true);
	  mandatory_assert(r == 0);
	  if (tmpa.remove_fn)
	      (tmpa.remove_fn)(c, &tmpa, status);
      } else {
        mandatory_assert(0);
      }

      if(force < 2)
        force = 0;
    } else if(force == 0){
      break;
    }
  }
}

// async get, checkasync() will eventually check reply
// against wanted.
void
aget(struct child *c, const Str &key, const Str &wanted, get_async_cb fn)
{
  if((c->nw % (window/2)) == 0)
    c->conn->flush();
  while(c->nw - c->nr >= window)
    checkasync(c, 1);

  c->conn->sendgetwhole(key, c->nw % SEQMOD);
  if (c->udp)
    c->conn->flush();

  struct async *a = &c->a[c->nw % window];
  a->cmd = Cmd_Get;
  a->seq = c->nw % SEQMOD;
  a->get_fn = (fn ? fn : defaultget);
  assert(key.len < int(sizeof(a->key)) - 1);
  memcpy(a->key, key.s, key.len);
  a->key[key.len] = 0;
  a->wantedlen = wanted.len;
  int wantedavail = std::min(wanted.len, int(sizeof(a->wanted)));
  memcpy(a->wanted, wanted.s, wantedavail);
  a->acked = 0;

  c->nw += 1;
}

void
aget(struct child *c, long ikey, long iwanted, get_async_cb fn)
{
    quick_istr key(ikey), wanted(iwanted);
    aget(c, key.string(), wanted.string(), fn);
}

int
put(struct child *c, const Str &key, const Str &val)
{
  mandatory_assert(c->nw == c->nr);

  unsigned int sseq = key.s[0]; // XXX
  c->conn->sendputwhole(key, val, sseq);
  c->conn->flush();

  unsigned int rseq = 0;
  int ret = c->conn->recvput(&rseq, false);
  mandatory_assert(ret == 0);
  mandatory_assert(rseq == sseq);

  return 0;
}

void
aput(struct child *c, const Str &key, const Str &val,
     put_async_cb fn = 0, const Str &wanted = Str())
{
  if((c->nw % (window/2)) == 0)
    c->conn->flush();
  while(c->nw - c->nr >= window)
    checkasync(c, 1);

  c->conn->sendputwhole(key, val, c->nw % SEQMOD, fn != 0);
  if (c->udp)
    c->conn->flush();

  struct async *a = &c->a[c->nw % window];
  a->cmd = (fn != 0 ? Cmd_Put_Status : Cmd_Put);
  a->seq = c->nw % SEQMOD;
  assert(key.len < int(sizeof(a->key)) - 1);
  memcpy(a->key, key.s, key.len);
  a->key[key.len] = 0;
  a->put_fn = fn;
  if (fn) {
      assert(wanted.len <= int(sizeof(a->wanted)));
      a->wantedlen = wanted.len;
      memcpy(a->wanted, wanted.s, wanted.len);
  } else {
      a->wantedlen = -1;
      a->wanted[0] = 0;
  }
  a->acked = 0;

  c->nw += 1;
}

bool
remove(struct child *c, const Str &key)
{
  mandatory_assert(c->nw == c->nr);

  unsigned int sseq = key.s[0]; // XXX
  c->conn->sendremove(key, sseq);
  c->conn->flush();

  unsigned int rseq = 0;
  int status = 0;
  int ret = c->conn->recvremove(&status, &rseq, false);
  mandatory_assert(ret == 0);
  mandatory_assert(rseq == sseq);

  return status;
}

void
aremove(struct child *c, const Str &key, remove_async_cb fn)
{
  if((c->nw % (window/2)) == 0)
    c->conn->flush();
  while(c->nw - c->nr >= window)
    checkasync(c, 1);

  c->conn->sendremove(key, c->nw % SEQMOD);
  if (c->udp)
    c->conn->flush();

  struct async *a = &c->a[c->nw % window];
  a->cmd = Cmd_Remove;
  a->seq = c->nw % SEQMOD;
  assert(key.len < int(sizeof(a->key)) - 1);
  memcpy(a->key, key.s, key.len);
  a->key[key.len] = 0;
  a->acked = 0;
  a->remove_fn = fn;

  c->nw += 1;
}

void
ascan(struct child *c, int numpairs, const char *key)
{
  if((c->nw % (window/2)) == 0)
    c->conn->flush();
  while(c->nw - c->nr >= window)
    checkasync(c, 1);

  c->conn->sendscanwhole(numpairs, key, c->nw % SEQMOD);
  if (c->udp)
    c->conn->flush();

  struct async *a = &c->a[c->nw % window];
  a->cmd = Cmd_Scan;
  a->seq = c->nw % SEQMOD;
  strncpy(a->key, key, sizeof(a->key));
  a->wantedlen = numpairs;
  a->acked = 0;

  c->nw += 1;
}

struct kvtest_client {
    kvtest_client(struct child *c, int id)
	: c_(c), id_(id) {
    }
    int id() const {
	return id_;
    }
    int nthreads() const {
	return ::children;
    }
    char minkeyletter() const {
        return ::minkeyletter;
    }
    char maxkeyletter() const {
        return ::maxkeyletter;
    }
    void register_timeouts(int n) {
	(void) n;
    }
    bool timeout(int which) const {
	return ::timeout[which];
    }
    uint64_t limit() const {
	return ::limit;
    }
    int getratio() const {
        assert(::getratio >= 0);
        return ::getratio;
    }
    uint64_t nkeys() const {
        return ::nkeys;
    }
    int keylen() const {
        return ::keylen;
    }
    int prefixLen() const {
        return ::prefixLen;
    }
    double now() const {
	return ::now();
    }

    void get(long ikey, Str *value) {
	quick_istr key(ikey);
	aget(c_, key.string(),
	     Str(reinterpret_cast<const char *>(&value), sizeof(value)),
	     asyncgetcb);
    }
    void get(const Str &key, int *ivalue) {
	aget(c_, key,
	     Str(reinterpret_cast<const char *>(&ivalue), sizeof(ivalue)),
	     asyncgetcb_int);
    }
    bool get_sync(long ikey) {
	char got[512];
	quick_istr key(ikey);
	return ::get(c_, key.string(), got, sizeof(got)) >= 0;
    }
    void get_check(long ikey, long iexpected) {
	aget(c_, ikey, iexpected, 0);
    }
    void get_check(const char *key, const char *val) {
	aget(c_, Str(key), Str(val), 0);
    }
    void get_check(const Str &key, const Str &val) {
	aget(c_, key, val, 0);
    }
    void get_check_key8(long ikey, long iexpected) {
	quick_istr key(ikey, 8), expected(iexpected);
	aget(c_, key.string(), expected.string(), 0);
    }
    void get_check_key10(long ikey, long iexpected) {
	quick_istr key(ikey, 10), expected(iexpected);
	aget(c_, key.string(), expected.string(), 0);
    }
    void many_get_check(int, long [], long []) {
        assert(0);
    }
    void get_check_sync(long ikey, long iexpected) {
        char key[512], val[512], got[512];
        sprintf(key, "%010ld", ikey);
        sprintf(val, "%ld", iexpected);
        memset(got, 0, sizeof(got));
	::get(c_, Str(key), got, sizeof(got));
        if (strcmp(val, got)) {
            fprintf(stderr, "key %s, expected %s, got %s\n", key, val, got);
            mandatory_assert(0);
        }
    }

    void put(const Str &key, const Str &value) {
	aput(c_, key, value);
    }
    void put(const Str &key, const Str &value, int *status) {
	aput(c_, key, value,
	     asyncputcb,
	     Str(reinterpret_cast<const char *>(&status), sizeof(status)));
    }
    void put(const char *key, const char *value) {
	aput(c_, Str(key), Str(value));
    }
    void put(const Str &key, long ivalue) {
	quick_istr value(ivalue);
	aput(c_, key, value.string());
    }
    void put(long ikey, long ivalue) {
	quick_istr key(ikey), value(ivalue);
	aput(c_, key.string(), value.string());
    }
    void put_key8(long ikey, long ivalue) {
	quick_istr key(ikey, 8), value(ivalue);
	aput(c_, key.string(), value.string());
    }
    void put_key10(long ikey, long ivalue) {
	quick_istr key(ikey, 10), value(ivalue);
	aput(c_, key.string(), value.string());
    }
    void put_sync(long ikey, long ivalue) {
	quick_istr key(ikey, 10), value(ivalue);
	::put(c_, key.string(), value.string());
    }

    void remove(const Str &key) {
	aremove(c_, key, 0);
    }
    void remove(long ikey) {
	quick_istr key(ikey);
	remove(key.string());
    }
    bool remove_sync(long ikey) {
	quick_istr key(ikey);
	return ::remove(c_, key.string());
    }

    int ruscale_partsz() const {
        return ::rscale_partsz;
    }
    int ruscale_init_part_no() const {
        return ::rsinit_part;
    }
    long nseqkeys() const {
        return 16 * ::rscale_partsz;
    }
    void wait_all() {
	checkasync(c_, 2);
    }
    void puts_done() {
    }
    void rcu_quiesce() {
    }
    void notice(String s) {
	if (!quiet) {
	    if (!s.empty() && s.back() == '\n')
		s = s.substring(0, -1);
	    if (s.empty() || isspace((unsigned char) s[0]))
		fprintf(stderr, "%d%.*s\n", c_->childno, s.length(), s.data());
	    else
		fprintf(stderr, "%d %.*s\n", c_->childno, s.length(), s.data());
	}
    }
    void notice(const char *fmt, ...) {
	if (!quiet) {
	    va_list val;
	    va_start(val, fmt);
	    String x;
	    if (!*fmt || isspace((unsigned char) *fmt))
		x = String(c_->childno) + fmt;
	    else
		x = String(c_->childno) + String(" ") + fmt;
	    vfprintf(stderr, x.c_str(), val);
	    va_end(val);
	}
    }
    void report(const Json &result) {
	if (!quiet) {
	    StringAccum sa;
	    double dv;
	    if (result.count("puts"))
		sa << " total " << result.get("puts");
	    if (result.get("puts_per_sec", dv))
		sa.snprintf(100, " %.0f put/s", dv);
	    if (result.get("gets_per_sec", dv))
		sa.snprintf(100, " %.0f get/s", dv);
	    if (!sa.empty())
		notice(sa.take_string());
	}
	printf("%s\n", result.unparse().c_str());
    }
    kvrandom_random rand;
    struct child *c_;
    int id_;
};

void
sync_rw1(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_sync_rw1(cl);
}

void
rw1(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_rw1(cl);
}

void
wd1(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_wd1(10000000, 1, cl);
}

void
wd1m1(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_wd1(100000000, 1, cl);
}

void
wd1m2(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_wd1(1000000000, 4, cl);
}

void
wd1check(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_wd1_check(10000000, 1, cl);
}

void
wd1m1check(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_wd1_check(100000000, 1, cl);
}

void
wd1m2check(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_wd1_check(1000000000, 4, cl);
}

void
wd2(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_wd2(cl);
}

void
wd2check(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_wd2_check(cl);
}

void
tri1(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_tri1(10000000, 1, cl);
}

void
tri1check(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_tri1_check(10000000, 1, cl);
}

void
udp1(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_udp1(cl);
}

void
rw3(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_rw3(cl);
}

// do a bunch of inserts to sequentially decreasing keys,
// then check that they all showed up.
// different clients might use same key sometimes.
void
rw4(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_rw4(cl);
}

void
rw5(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_rw1fixed(cl);
}

void
rw16(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_rw16(cl);
}

int
xcompar(const void *xa, const void *xb)
{
  long *a = (long *) xa;
  long *b = (long *) xb;
  if(*a == *b)
    return 0;
  if(*a < *b)
    return -1;
  return 1;
}

void
w1(struct child *c)
{
    kvtest_client cl(c, c->childno);
    kvtest_w1_seed(cl, first_seed + c->childno);
}

// like w1, but in a binary-tree-like order that
// produces a balanced 3-wide tree.
void
w1b(struct child *c)
{
  int n;
  if (limit == ~(uint64_t) 0)
      n = 4000000;
  else
      n = std::min(limit, (uint64_t) INT_MAX);
  long *a = (long *) malloc(sizeof(long) * n);
  mandatory_assert(a);
  char *done = (char *) malloc(n);

  srandom(first_seed + c->childno);

  // insert in an order which causes 3-wide
  // to be balanced

  for(int i = 0; i < n; i++){
    a[i] = random();
    done[i] = 0;
  }

  qsort(a, n, sizeof(a[0]), xcompar);
  mandatory_assert(a[0] <= a[1] && a[1] <= a[2] && a[2] <= a[3]);

  double t0 = now(), t1;

  for(int stride = n / 2; stride > 0; stride /= 2){
    for(int i = stride; i < n; i += stride){
      if(done[i] == 0){
        done[i] = 1;
        char key[512], val[512];
        sprintf(key, "%010ld", a[i]);
        sprintf(val, "%ld", a[i] + 1);
        aput(c, Str(key), Str(val));
      }
    }
  }
  for(int i = 0; i < n; i++){
    if(done[i] == 0){
      done[i] = 1;
      char key[512], val[512];
      sprintf(key, "%010ld", a[i]);
      sprintf(val, "%ld", a[i] + 1);
      aput(c, Str(key), Str(val));
    }
  }

  checkasync(c, 2);
  t1 = now();

  free(done);
  free(a);
  Json result = Json().set("total", (long) (n / (t1 - t0)))
    .set("puts", n)
    .set("puts_per_sec", n / (t1 - t0));
  printf("%s\n", result.unparse().c_str());
}

// do four million gets.
// in a random order.
// if we get in the same order that w1 put, performance is
// about 15% better for b-tree.
void
r1(struct child *c)
{
    kvtest_client cl(c, c->childno);
    kvtest_r1_seed(cl, first_seed + c->childno);
}

// update the same small set of keys over and over,
// to uncover concurrent update bugs in the server.
void
same(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_same(cl);
}

// update random keys from a set of 10 million.
// maybe best to run it twice, first time to
// populate the database.
void
u1(struct child *c)
{
  int i, n;
  double t0 = now();

  srandom(first_seed + c->childno);

  for(i = 0; i < 10000000; i++){
    char key[512], val[512];
    long x = random() % 10000000;
    sprintf(key, "%ld", x);
    sprintf(val, "%ld", x + 1);
    aput(c, Str(key), Str(val));
  }
  n = i;

  checkasync(c, 2);

  double t1 = now();
  Json result = Json().set("total", (long) (n / (t1 - t0)))
    .set("puts", n)
    .set("puts_per_sec", n / (t1 - t0));
  printf("%s\n", result.unparse().c_str());
}

#define CPN 10000000

void
cpa(struct child *c)
{
  int i, n;
  double t0 = now();

  srandom(first_seed + c->childno);

  for(i = 0; i < CPN; i++){
    char key[512], val[512];
    long x = random();
    sprintf(key, "%ld", x);
    sprintf(val, "%ld", x + 1);
    aput(c, Str(key), Str(val));
  }
  n = i;

  checkasync(c, 2);

  double t1 = now();
  Json result = Json().set("total", (long) (n / (t1 - t0)))
    .set("puts", n)
    .set("puts_per_sec", n / (t1 - t0));
  printf("%s\n", result.unparse().c_str());

}

void
cpc(struct child *c)
{
  int i, n;
  double t0 = now();

  srandom(first_seed + c->childno);

  for(i = 0; !timeout[0]; i++){
    char key[512], val[512];
    if (i % CPN == 0)
      srandom(first_seed + c->childno);
    long x = random();
    sprintf(key, "%ld", x);
    sprintf(val, "%ld", x + 1);
    aget(c, Str(key), Str(val), NULL);
  }
  n = i;

  checkasync(c, 2);

  double t1 = now();
  Json result = Json().set("total", (long) (n / (t1 - t0)))
    .set("gets", n)
    .set("gets_per_sec", n / (t1 - t0));
  printf("%s\n", result.unparse().c_str());
}

void
cpd(struct child *c)
{
  int i, n;
  double t0 = now();

  srandom(first_seed + c->childno);

  for(i = 0; !timeout[0]; i++){
    char key[512], val[512];
    if (i % CPN == 0)
      srandom(first_seed + c->childno);
    long x = random();
    sprintf(key, "%ld", x);
    sprintf(val, "%ld", x + 1);
    aput(c, Str(key), Str(val));
  }
  n = i;

  checkasync(c, 2);

  double t1 = now();
  Json result = Json().set("total", (long) (n / (t1 - t0)))
    .set("puts", n)
    .set("puts_per_sec", n / (t1 - t0));
  printf("%s\n", result.unparse().c_str());
}

// multiple threads simultaneously update the same key.
// keep track of the winning value.
// use over2 to make sure it's the same after a crash/restart.
void
over1(struct child *c)
{
  int ret, iter;

  srandom(first_seed + c->childno);

  iter = 0;
  while(!timeout[0]){
    char key1[64], key2[64], val1[64], val2[64];
    time_t xt = time(0);
    while(xt == time(0))
      ;
    sprintf(key1, "%d", iter);
    sprintf(val1, "%ld", random());
    put(c, Str(key1), Str(val1));
    napms(500);
    ret = get(c, Str(key1), val2, sizeof(val2));
    mandatory_assert(ret > 0);
    sprintf(key2, "%d-%d", iter, c->childno);
    put(c, Str(key2), Str(val2));
    if(c->childno == 0)
      printf("%d: %s\n", iter, val2);
    iter++;
  }
  checkasync(c, 2);
  printf("0\n");
}

// check each round of over1()
void
over2(struct child *c)
{
  int iter;

  for(iter = 0; ; iter++){
    char key1[64], key2[64], val1[64], val2[64];
    int ret;
    sprintf(key1, "%d", iter);
    ret = get(c, Str(key1), val1, sizeof(val1));
    if(ret == -1)
      break;
    sprintf(key2, "%d-%d", iter, c->childno);
    ret = get(c, Str(key2), val2, sizeof(val2));
    if(ret == -1)
      break;
    if(c->childno == 0)
      printf("%d: %s\n", iter, val2);
    mandatory_assert(strcmp(val1, val2) == 0);
  }

  checkasync(c, 2);
  fprintf(stderr, "child %d checked %d\n", c->childno, iter);
  printf("0\n");
}

// do a bunch of inserts to distinct keys.
// rec2() checks that a prefix of those inserts are present.
// meant to be interrupted by a crash/restart.
void
rec1(struct child *c)
{
  int i;
  double t0 = now(), t1;

  srandom(first_seed + c->childno);

  for(i = 0; !timeout[0]; i++){
    char key[512], val[512];
    long x = random();
    sprintf(key, "%ld-%d-%d", x, i, c->childno);
    sprintf(val, "%ld", x);
    aput(c, Str(key), Str(val));
  }
  checkasync(c, 2);
  t1 = now();

  fprintf(stderr, "child %d: done %d %.0f put/s\n",
          c->childno,
          i,
          i / (t1 - t0));
  printf("%.0f\n", i / (t1 - t0));
}

void
rec2(struct child *c)
{
  int i;

  srandom(first_seed + c->childno);

  for(i = 0; ; i++){
    char key[512], val[512], wanted[512];
    long x = random();
    sprintf(key, "%ld-%d-%d", x, i, c->childno);
    sprintf(wanted, "%ld", x);
    int ret = get(c, Str(key), val, sizeof(val));
    if(ret == -1)
      break;
    val[ret] = 0;
    if(strcmp(val, wanted) != 0){
      fprintf(stderr, "oops key %s got %s wanted %s\n", key, val, wanted);
      exit(1);
    }
  }

  int i0 = i; // first missing record
  for(i = i0+1; i < i0 + 10000; i++){
    char key[512], val[512];
    long x = random();
    sprintf(key, "%ld-%d-%d", x, i, c->childno);
    val[0] = 0;
    int ret = get(c, Str(key), val, sizeof(val));
    if(ret != -1){
      printf("child %d: oops first missing %d but %d present\n",
             c->childno, i0, i);
      exit(1);
    }
  }
  checkasync(c, 2);

  fprintf(stderr, "correct prefix of %d records\n", i0);
  printf("0\n");
}

// ask server to checkpoint
void
cpb(struct child *c)
{
  c->conn->cp(c->childno);
}

// mimic the first benchmark from the VoltDB blog:
//   https://voltdb.com/blog/key-value-benchmarking
//   https://voltdb.com/blog/key-value-benchmark-faq
//   http://community.voltdb.com/kvbenchdetails
//   svn checkout http://svnmirror.voltdb.com/projects/kvbench/trunk
// 500,000 items: 50-byte key, 12 KB value
// volt1a creates the DB.
// volt1b runs the benchmark.
#define VOLT1N 500000
#define VOLT1SIZE (12*1024)
void
volt1a(struct child *c)
{
  int i, j;
  double t0 = now(), t1;
  char *val = (char *) malloc(VOLT1SIZE + 1);
  mandatory_assert(val);

  srandom(first_seed + c->childno);

  for(i = 0; i < VOLT1SIZE; i++)
    val[i] = 'a' + (i % 26);
  val[VOLT1SIZE] = '\0';

  // XXX insert the keys in a random order to maintain
  // tree balance.
  int *keys = (int *) malloc(sizeof(int) * VOLT1N);
  mandatory_assert(keys);
  for(i = 0; i < VOLT1N; i++)
    keys[i] = i;
  for(i = 0; i < VOLT1N; i++){
    int x = random() % VOLT1N;
    int tmp = keys[i];
    keys[i] = keys[x];
    keys[x] = tmp;
  }

  for(i = 0; i < VOLT1N; i++){
    char key[100];
    sprintf(key, "%-50d", keys[i]);
    for(j = 0; j < 20; j++)
      val[j] = 'a' + (j % 26);
    sprintf(val, ">%d", keys[i]);
    int j = strlen(val);
    val[j] = '<';
    mandatory_assert(strlen(val) == VOLT1SIZE);
    mandatory_assert(strlen(key) == 50);
    mandatory_assert(isdigit(key[0]));
    aput(c, Str(key), Str(val));
  }
  checkasync(c, 2);
  t1 = now();

  free(val);
  free(keys);

  Json result = Json().set("total", (long) (i / (t1 - t0)));
  printf("%s\n", result.unparse().c_str());
}

// the actual volt1 benchmark.
// get or update with equal probability.
// their client pipelines many requests.
// they use 8 client threads.
// blog post says, for one server, VoltDB 17000, Cassandra 9740
// this benchmark ends up being network or disk limited,
// due to the huge values.
void
volt1b(struct child *c)
{
  int i, n, j;
  double t0 = now(), t1;
  char *wanted = (char *) malloc(VOLT1SIZE + 1);
  mandatory_assert(wanted);

  for(i = 0; i < VOLT1SIZE; i++)
    wanted[i] = 'a' + (i % 26);
  wanted[VOLT1SIZE] = '\0';

  srandom(first_seed + c->childno);

  for(i = 0; !timeout[0]; i++){
    char key[100];
    int x = random() % VOLT1N;
    sprintf(key, "%-50d", x);
    for(j = 0; j < 20; j++)
      wanted[j] = 'a' + (j % 26);
    sprintf(wanted, ">%d", x);
    int j = strlen(wanted);
    wanted[j] = '<';
    if(i > 1)
      checkasync(c, 1); // try to avoid deadlock, only 2 reqs outstanding
    if((random() % 2) == 0)
	aget(c, Str(key, 50), Str(wanted, VOLT1SIZE), 0);
    else
	aput(c, Str(key, 50), Str(wanted, VOLT1SIZE));
  }
  n = i;

  checkasync(c, 2);
  t1 = now();

  Json result = Json().set("total", (long) (n / (t1 - t0)));
  printf("%s\n", result.unparse().c_str());
}

// second VoltDB benchmark.
// 500,000 pairs, 50-byte key, value is 50 32-bit ints.
// pick a key, read one int, if odd, write a different int (same key).
// i'm simulating columns by embedding column name in key: rowname-colname.
// also the read/modify/write is not atomic.
// volt2a creates the DB.
// volt2b runs the benchmark.
#define VOLT2N 500000
#define VOLT2INTS 50
void
volt2a(struct child *c)
{
  int i, j, n = 0;
  double t0 = now(), t1;

  srandom(first_seed + c->childno);

  // XXX insert the keys in a random order to maintain
  // tree balance.
  int *keys = (int *) malloc(sizeof(int) * VOLT2N);
  mandatory_assert(keys);
  for(i = 0; i < VOLT2N; i++)
    keys[i] = i;
  for(i = 0; i < VOLT2N; i++){
    int x = random() % VOLT2N;
    int tmp = keys[i];
    keys[i] = keys[x];
    keys[x] = tmp;
  }

  int subkeys[VOLT2INTS];
  for(i = 0; i < VOLT2INTS; i++)
    subkeys[i] = i;
  for(i = 0; i < VOLT2INTS; i++){
    int x = random() % VOLT2INTS;
    int tmp = subkeys[i];
    subkeys[i] = subkeys[x];
    subkeys[x] = tmp;
  }

  for(i = 0; i < VOLT2N; i++){
    for(j = 0; j < VOLT2INTS; j++){
      char val[32], key[100];
      int k;
      sprintf(key, "%d-%d", keys[i], subkeys[j]);
      for(k = strlen(key); k < 50; k++)
        key[k] = ' ';
      key[50] = '\0';
      sprintf(val, "%ld", random());
      aput(c, Str(key), Str(val));
      n++;
    }
  }
  checkasync(c, 2);
  t1 = now();

  free(keys);
  Json result = Json().set("total", (long) (n / (t1 - t0)));
  printf("%s\n", result.unparse().c_str());
}

// get callback
void
volt2b1(struct child *c, struct async *a, bool, const Str &val)
{
  int k = atoi(a->key);
  int v = atoi(val.s);
  if((v % 2) == 1){
    char key[100], val[100];
    sprintf(key, "%d-%ld", k, random() % VOLT2INTS);
    for (int i = strlen(key); i < 50; i++)
      key[i] = ' ';
    sprintf(val, "%ld", random());
    aput(c, Str(key, 50), Str(val));
  }
}

void
volt2b(struct child *c)
{
  int i, n;
  double t0 = now(), t1;
  srandom(first_seed + c->childno);
  for(i = 0; !timeout[0]; i++){
    char key[100];
    int x = random() % VOLT2N;
    int y = random() % VOLT2INTS;
    sprintf(key, "%d-%d", x, y);
    int j;
    for(j = strlen(key); j < 50; j++)
      key[j] = ' ';
    aget(c, Str(key, 50), Str(), volt2b1);
  }
  n = i;

  checkasync(c, 2);
  t1 = now();

  Json result = Json().set("total", (long) (n / (t1 - t0)));
  printf("%s\n", result.unparse().c_str());
}

void
scantest(struct child *c)
{
  int i, ret;

  srandom(first_seed + c->childno);

  for(i = 100; i < 200; i++){
    char key[32], val[32];
    int kl = sprintf(key, "k%04d", i);
    sprintf(val, "v%04d", i);
    aput(c, Str(key, kl), Str(val));
  }

  checkasync(c, 2);

  int n;
  vector<string> keys;
  vector< vector<string> > vals;

  for(i = 90; i < 210; i++){
    char key[32];
    sprintf(key, "k%04d", i);
    int wanted = random() % 10;
    c->conn->sendscanwhole(wanted, key, 1);
    c->conn->flush();
    keys.clear();
    vals.clear();
    ret = c->conn->recvscan(keys, vals, NULL, false);
    mandatory_assert(ret == 0);
    n = keys.size();
    if(i <= 200 - wanted){
      mandatory_assert(n == wanted);
    } else if(i <= 200){
      mandatory_assert(n == 200 - i);
    } else {
      mandatory_assert(n == 0);
    }
    int k0 = (i < 100 ? 100 : i);
    int j, ki;
    for(j = k0, ki = 0; j < k0 + wanted && j < 200; j++, ki++){
      char xkey[32], xval[32];
      sprintf(xkey, "k%04d", j);
      sprintf(xval, "v%04d", j);
      if (strcmp(keys[ki].c_str(), xkey) != 0) {
	fprintf(stderr, "Assertion failed @%d: strcmp(%s, %s) == 0\n", ki, keys[ki].c_str(), xkey);
	mandatory_assert(0);
      }
      mandatory_assert(strcmp(vals[ki][0].c_str(), xval) == 0);
    }

    sprintf(key, "k%04d-a", i);
    c->conn->sendscanwhole(1, key, 1);
    c->conn->flush();
    keys.clear();
    vals.clear();
    ret = c->conn->recvscan(keys, vals, NULL, false);
    mandatory_assert(ret == 0);
    n = keys.size();
    if(i >= 100 && i < 199){
      mandatory_assert(n == 1);
      sprintf(key, "k%04d", i+1);
      mandatory_assert(strcmp(keys[0].c_str(), key) == 0);
    }
  }

  c->conn->sendscanwhole(10, "k015", 1);
  c->conn->flush();
  keys.clear();
  vals.clear();
  ret = c->conn->recvscan(keys, vals, NULL, false);
  mandatory_assert(ret == 0);
  n = keys.size();
  mandatory_assert(n == 10);
  mandatory_assert(strcmp(keys[0].c_str(), "k0150") == 0);
  mandatory_assert(strcmp(vals[0][0].c_str(), "v0150") == 0);

  fprintf(stderr, "scantest OK\n");
  printf("0\n");
}

void
rw2(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_rw2(cl);
}

void
wscale(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_wscale(cl);
}

void
ruscale_init(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_ruscale_init(cl);
}

void
rscale(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_rscale(cl);
}

void
uscale(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_uscale(cl);
}
void
long_go(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_long_go(cl);
}

void
long_init(struct child *c)
{
    kvtest_client cl(c, first_seed + c->childno);
    kvtest_long_init(cl);
}

