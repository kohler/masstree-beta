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
#ifndef KVC_HH
#define KVC_HH 1
#include "kvproto.hh"
#include "kvrow.hh"
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <string>
#include <queue>
#include <vector>

class KVConn {
  public:
    KVConn(const char *server, int port, int target_core = -1) {
        kvbuf_ = new_bufkvout();
        struct hostent *ent = gethostbyname(server);
        always_assert(ent);
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        always_assert(fd > 0);
        fdtoclose_ = fd;
        int yes = 1;
        always_assert(fd >= 0);
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

        struct sockaddr_in sin;
        memset(&sin, 0, sizeof(sin));
        sin.sin_family = AF_INET;
        sin.sin_port = htons(port);
        memcpy(&sin.sin_addr.s_addr, ent->h_addr, ent->h_length);
        int r = connect(fd, (const struct sockaddr *)&sin, sizeof(sin));
        if (r) {
            perror("connect");
            exit(EXIT_FAILURE);
        }

        in_ = new_kvin(fd, 64*1024);
        out_ = new_kvout(fd, 64*1024);
        handshake(target_core);
    }
    KVConn(int fd, bool tcp) {
        kvbuf_ = new_bufkvout();
        in_ = new_kvin(fd, 64*1024);
        out_ = new_kvout(fd, 64*1024);
        fdtoclose_ = -1;
        if (tcp)
            handshake(-1);
    }
    ~KVConn() {
        if (fdtoclose_ >= 0)
            close(fdtoclose_);
        free_kvin(in_);
        free_kvout(out_);
        free_kvout(kvbuf_);
    }
    void sendgetwhole(Str key, unsigned int seq) {
        row_type::fields_type f;
        row_type::make_get1_fields(f);
        sendget(key, f, seq);
    }
    void sendget(Str key, const row_type::fields_type& f, unsigned int seq) {
        KVW(out_, (int)Cmd_Get);
        KVW(out_, seq);
	if (key.len > MaxKeyLen) {
	    fprintf(stderr, "maxkeylen %d, %*.s\n", key.len, key.len, key.s);
	    exit(EXIT_FAILURE);
	}
        kvwrite_str(out_, key);
        // Write fields
        kvout_reset(kvbuf_);
        row_type::kvwrite_fields(kvbuf_, f);
        kvwrite_str(out_, Str(kvbuf_->buf, kvbuf_->n));
    }

    // return the length of the value if successful (>=0);
    // -1 if I/O error
    // -2 if the key is not found
    int recvget(std::vector<std::string> &row, unsigned int *seq, bool gotseq) {
        if(!gotseq && kvread(in_, (char*)seq, sizeof(*seq)) != sizeof(*seq))
            return -1;
        if (kvread_row(in_, row) < 0)
            return -1;
        return 0;
    }

    void sendputwhole(Str key, Str val, unsigned int seq,
		      bool need_status = false) {
        row_type::change_type c;
        row_type::make_put1_change(c, val);
        sendput(key, c, seq, need_status);
    }
    void sendput(Str key, row_type::change_type& c, unsigned int seq,
		 bool need_status = false) {
	row_type::sort(c);
        KVW(out_, (int) (need_status ? Cmd_Put_Status : Cmd_Put));
        KVW(out_, seq);
	if (key.len > MaxKeyLen) {
	    fprintf(stderr, "maxkeylen %d, %.*s\n", key.len, key.len, key.s);
	    exit(EXIT_FAILURE);
	}
        kvwrite_str(out_, key);
        // write change
        kvout_reset(kvbuf_);
        row_type::kvwrite_change(kvbuf_, c);
        kvwrite_str(out_, Str(kvbuf_->buf, kvbuf_->n));
    }
    void sendremove(Str key, unsigned int seq) {
        KVW(out_, (int)Cmd_Remove);
        KVW(out_, seq);
	if (key.len > MaxKeyLen) {
	    fprintf(stderr, "maxkeylen %d, %.*s\n", key.len, key.len, key.s);
	    exit(EXIT_FAILURE);
	}
        kvwrite_str(out_, key);
    }
    int recvput(unsigned int *seq, bool gotseq) {
        if(!gotseq && kvread(in_, (char*)seq, sizeof(*seq)) != sizeof(*seq))
            return -1;
        return 0;
    }
    int recvputstatus(int *status, unsigned int *seq, bool gotseq) {
        if(!gotseq && kvread(in_, (char*)seq, sizeof(*seq)) != sizeof(*seq))
            return -1;
	if (kvread(in_, (char *) status, sizeof(*status)) != sizeof(*status))
	    return -1;
        return 0;
    }
    int recvremove(int *status, unsigned int *seq, bool gotseq) {
        if (!gotseq && kvread(in_, (char*)seq, sizeof(*seq)) != sizeof(*seq))
            return -1;
	if (kvread(in_, (char *) status, sizeof(*status)) != sizeof(*status))
	    return -1;
        return 0;
    }
    void sendscanwhole(Str firstkey, int numpairs, unsigned int seq) {
        row_type::fields_type f;
        row_type::make_get1_fields(f);
        sendscan(firstkey, f, numpairs, seq);
    }
    void sendscan(Str firstkey, const row_type::fields_type& f, int numpairs,
                  unsigned int seq) {
        KVW(out_, (int)Cmd_Scan);
        KVW(out_, seq);
        kvwrite_str(out_, firstkey);
        // write fields
        kvout_reset(kvbuf_);
        row_type::kvwrite_fields(kvbuf_, f);
        kvwrite_str(out_, Str(kvbuf_->buf, kvbuf_->n));
        // write numpairs
        KVW(out_, numpairs);
    }
    int recvscan(std::vector<std::string> &keys,
                 std::vector<std::vector<std::string> > &rows,
	         unsigned int *seq, bool gotseq) {
        if(!gotseq) {
            unsigned int tmpseq;
            if (!seq)
                seq = &tmpseq;
            if (kvread(in_, (char*)seq, sizeof(*seq)) != sizeof(*seq))
                return -1;
        }
        while(1){
            char keybuf[MaxKeyLen];
            int keylen;
            if (kvread_str(in_, keybuf, MaxKeyLen, keylen) < 0)
                return -1;
            if (keylen == 0)
                return 0;
            keys.push_back(std::string(keybuf, keylen));
            std::vector<std::string> row;
            if (kvread_row(in_, row) < 0)
                return -1;
            rows.push_back(std::move(row));
        }
    }

    void checkpoint(int childno) {
	always_assert(childno == 0);
        fprintf(stderr, "asking for a checkpoint\n");
        KVW(out_, (int)Cmd_Checkpoint);
        KVW(out_, (int)0);
        flush();
        printf("sent\n");
    }

    void flush() {
        kvflush(out_);
    }
    void readseq(unsigned int *seq) {
        ssize_t r = kvread(in_, (char*)seq, sizeof(*seq));
        always_assert((size_t) r == sizeof(*seq));
    }

    int check(int tryhard) {
        return kvcheck(in_, tryhard);
    }

  private:
    struct kvin *in_;
    struct kvout *out_;
    struct kvout *kvbuf_;

    int fdtoclose_;
    int partition_;

    void handshake(int target_core) {
        KVW(out_, target_core);
        struct kvproto p;
        kvproto_init(p);
        KVW(out_, p);
        kvflush(out_);
        bool ok;
        int r = KVR(in_, ok);
	r = KVR(in_, partition_);
	if (r <= 0) {
	    fprintf(stderr, "Connection closed by a crashed server?\n");
	    exit(EXIT_FAILURE);
	}
        if (!ok) {
            fprintf(stderr, "Incompatible kvdb protocol. Make sure the "
                    "client uses the same row type as the kvd.\n");
            exit(EXIT_FAILURE);
        }
    }
};

#endif
