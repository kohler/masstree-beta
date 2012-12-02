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
// buffered read and write for kvc/kvd.
// stdio is good but not quite what I want.
// need to be able to check if any input
// available, and do non-blocking check.
// also, fwrite just isn't very fast, at
// least on the Mac.

#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/select.h>
#include <sys/time.h>
#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include "kvio.hh"

// API to allocate a new kvin.
struct kvin *
new_kvin(int fd, int buflen)
{
  struct kvin *kvin = (struct kvin *) malloc(sizeof(struct kvin));
  assert(kvin);
  bzero(kvin, sizeof(struct kvin));
  kvin->len = buflen;
  kvin->buf = (char *) malloc(kvin->len);
  assert(kvin->buf);
  kvin->fd = fd;
  return kvin;
}

// Allocate a kvin for an existing buffer, no fd.
struct kvin *
new_bufkvin(char *buf)
{
  struct kvin *kvin = (struct kvin *) malloc(sizeof(struct kvin));
  assert(kvin);
  bzero(kvin, sizeof(struct kvin));
  kvin->len = 0;
  kvin->buf = buf;
  kvin->fd = -1;
  return kvin;
}

void
kvin_init(struct kvin *kvin, char *buf, int len)
{
  bzero(kvin, sizeof(struct kvin));
  kvin->buf = buf;
  kvin->fd = -1;
  kvin_setlen(kvin, len);
}

void
kvin_setlen(struct kvin *kvin, int len)
{
  assert(kvin->fd == -1);
  kvin->len = len;
  kvin->i0 = 0;
  kvin->i1 = len;
}

char *
kvin_skip(struct kvin *kvin, int n)
{
  assert(kvin->fd == -1);
  char *p = &kvin->buf[kvin->i0];
  kvin->i0 += n;
  assert(kvin->i0 <= kvin->i1);
  return p;
}

// API to free a kvin.
// does not close() the fd.
void
free_kvin(struct kvin *kvin)
{
  if(kvin->buf)
    free(kvin->buf);
  kvin->buf = 0;
  free(kvin);
}

// internal.
// do a read().
static int
kvreallyread(struct kvin *kvin)
{
  if(kvin->i0 == kvin->i1)
    kvin->i0 = kvin->i1 = 0;
  int wanted = kvin->len - kvin->i1;
  assert(kvin->fd >= 0);
  assert(wanted > 0 && kvin->i1 + wanted <= kvin->len);
  int cc = read(kvin->fd, kvin->buf + kvin->i1, wanted);
  if (cc < 0)
      return -1;
  kvin->i1 += cc;
  return kvin->i1 - kvin->i0;
}

// API to read exactly n bytes.
// return n or -1.
int
kvread(struct kvin *kvin, char *buf, int n)
{
  int i = 0;
  while(i < n){
    if(kvin->i1 > kvin->i0){
      int cc = kvin->i1 - kvin->i0;
      if(cc > n - i)
        cc = n - i;
      bcopy(kvin->buf + kvin->i0, buf + i, cc);
      i += cc;
      kvin->i0 += cc;
    } else {
      if(kvreallyread(kvin) < 1)
        return -1;
    }
  }
  return n;
}

// API to see how much data is waiting.
// tryhard=0 -> just check buffer.
// tryhard=1 -> do non-blocking read if buf is empty.
// tryhard=2 -> block if buf is empty.
int
kvcheck(struct kvin *kvin, int tryhard)
{
  if (kvin->i1 == kvin->i0 && tryhard) {
    assert(kvin->fd >= 0);
    if(tryhard == 2){
      // blocking read
      if (kvreallyread(kvin) <= 0)
        return -1;
    } else if(tryhard == 1){
      // non-blocking read
      mandatory_assert(kvin->fd < FD_SETSIZE);
      fd_set rfds;
      FD_ZERO(&rfds);
      FD_SET(kvin->fd, &rfds);
      struct timeval tv;
      tv.tv_sec = 0;
      tv.tv_usec = 0;
      select(kvin->fd + 1, &rfds, NULL, NULL, &tv);
      if(FD_ISSET(kvin->fd, &rfds))
        kvreallyread(kvin);
    }
  }
  return kvin->i1 - kvin->i0;
}

// read from socket once.
// return the number of bytes available in the socket; -1 on error
int
mayblock_kvoneread(struct kvin *kvin) {
  if (kvreallyread(kvin) <= 0)
    return -1;
  return kvin->i1 - kvin->i0;
}

// API to allocate a new kvout.
struct kvout *
new_kvout(int fd, int buflen)
{
  struct kvout *kvout = (struct kvout *) malloc(sizeof(struct kvout));
  assert(kvout);
  bzero(kvout, sizeof(struct kvout));
  kvout->len = buflen;
  kvout->buf = (char *) malloc(kvout->len);
  assert(kvout->buf);
  kvout->fd = fd;
  return kvout;
}

// API to allocate a new kvout for a buffer, no fd.
struct kvout *
new_bufkvout()
{
  struct kvout *kvout = (struct kvout *) malloc(sizeof(struct kvout));
  assert(kvout);
  bzero(kvout, sizeof(struct kvout));
  kvout->len = 256;
  kvout->buf = (char *) malloc(kvout->len);
  assert(kvout->buf);
  kvout->n = 0;
  kvout->fd = -1;
  return kvout;
}

// API to clear out a buf kvout.
void
kvout_reset(struct kvout *kvout)
{
  assert(kvout->fd < 0);
  kvout->n = 0;
}

// API to free a kvout.
// does not close() the fd.
void
free_kvout(struct kvout *kvout)
{
  if(kvout->buf)
    free(kvout->buf);
  kvout->buf = 0;
  free(kvout);
}

void
kvflush(struct kvout *kvout)
{
  assert(kvout->fd >= 0);
  size_t sent = 0;
  while (kvout->n > sent) {
    ssize_t cc = write(kvout->fd, kvout->buf + sent, kvout->n - sent);
    if(cc <= 0) {
      if(errno == EWOULDBLOCK) {
        usleep(1);
        continue;
      }
      perror("kvflush write");
      return;
    }
    sent += cc;
  }
  kvout->n = 0;
}

// API
int
kvwrite(struct kvout *kvout, const void *buf, unsigned int n)
{
  if(kvout->n + n > kvout->len){
    if(kvout->fd >= 0){
      kvflush(kvout);
    } else {
      while(kvout->n + n > kvout->len)
        kvout->len *= 2;
      kvout->buf = (char *) realloc(kvout->buf, kvout->len);
      assert(kvout->buf);
    }
  }
  assert(kvout->n + n <= kvout->len);
  bcopy(buf, kvout->buf + kvout->n, n);
  kvout->n += n;
  return n;
}
