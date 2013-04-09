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

using namespace std;

class KVCallback {
  public:
    KVCallback() {
        _ref = 1;
    }
    virtual ~KVCallback() {
    }
    virtual void incref() {
        _ref ++;
    }
    virtual void decref() {
        _ref --;
        if (_ref == 0)
            delete this;
    }
    virtual bool callback_get(int r, vector<string> &val) {
        (void) r, (void) val;
        return true;
    }
    virtual bool callback_put(int r) {
        (void) r;
        return true;
    }
    virtual bool callback_remove(int r) {
        (void) r;
        return true;
    }
    virtual bool callback_scan(int r, vector<string> &keys,
                               vector< vector<string> > &vals) {
        (void) r, (void) keys, (void) vals;
        return true;
    }
    int _cmd;
    unsigned int _wantedlen;
  private:
    int _ref;
};

class KVConn {
  public:
    KVConn(const char *server, int port, int target_core = -1) {
        kvbuf = new_bufkvout();
        struct hostent *ent = gethostbyname(server);
        mandatory_assert(ent);
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        mandatory_assert(fd > 0);
        this->fdtoclose = fd;
        int yes = 1;
        mandatory_assert(fd >= 0);
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
        in = new_kvin(fd, 64*1024);
        out = new_kvout(fd, 64*1024);
        KVW(out, target_core);
        handshake();
    }
    KVConn(int fd, bool tcp) {
        kvbuf = new_bufkvout();
        in = new_kvin(fd, 64*1024);
        out = new_kvout(fd, 64*1024);
        this->fdtoclose = -1;
        if (tcp) {
            KVW(out, -1);
            handshake();
        }
    }
    ~KVConn() {
        if (fdtoclose >= 0)
            close(fdtoclose);
        free_kvin(in);
        free_kvout(out);
        free_kvout(kvbuf);
    }
    void aput(const Str &key, row_type::change_t &c, KVCallback *cb) {
        cb->incref();
        cb->_cmd = Cmd_Put;
        reqs.push(cb);
        sendput(key, c, 0);
    }
    void aget(const Str &key, row_type::fields_t &f, KVCallback *cb) {
        cb->incref();
        cb->_cmd = Cmd_Get;
        reqs.push(cb);
        sendget(key, f, 0);
    }
    void aget(const char *key, row_type::fields_t &f, KVCallback *cb) {
	aget(Str(key, strlen(key)), f, cb);
    }
    void ascan(int numpairs, const char *key, row_type::fields_t &f,
               KVCallback *cb) {
        cb->incref();
        cb->_cmd = Cmd_Scan;
        cb->_wantedlen = numpairs;
        reqs.push(cb);
        sendscan(numpairs, key, f, 0);
    }
    void run() {
        if (reqs.size() == 0)
            return;
        flush();
        kvcheck(in, 2);
        receive();
    }
    bool drain() {
        if (reqs.size() == 0)
            return true;
        flush();
        run();
        return false;
    }
    void sendgetwhole(const Str &key, unsigned int seq) {
        row_type::fields_t f;
        row_type::make_get1_fields(f);
        sendget(key, f, seq);
    }
    void sendget(const Str &key, row_type::fields_t &f, unsigned int seq) {
        KVW(out, (int)Cmd_Get);
        KVW(out, seq);
	if (key.len > MaxKeyLen) {
	    fprintf(stderr, "maxkeylen %d, %*.s\n", key.len, key.len, key.s);
	    exit(EXIT_FAILURE);
	}
        kvwrite_str(out, key);
        // Write fields
        kvout_reset(kvbuf);
        row_type::kvwrite_fields(kvbuf, f);
        kvwrite_str(out, Str(kvbuf->buf, kvbuf->n));
    }
    void sendget(const char *key, row_type::fields_t &f, unsigned int seq) {
	sendget(Str(key, strlen(key)), f, seq);
    }

    // return the length of the value if successful (>=0);
    // -1 if I/O error
    // -2 if the key is not found
    int recvget(vector<string> &row, unsigned int *seq, bool gotseq) {
        if(!gotseq && kvread(in, (char*)seq, sizeof(*seq)) != sizeof(*seq))
            return -1;
        if (kvread_row(in, row) < 0)
            return -1;
        return 0;
    }

    void sendputwhole(const Str &key, const Str &val, unsigned int seq,
		      bool need_status = false) {
        row_type::change_t c;
        row_type::make_put1_change(c, val);
        sendput(key, c, seq, need_status);
    }
    void sendput(const Str &key, row_type::change_t &c, unsigned int seq,
		 bool need_status = false) {
	row_type::sort(c);
        KVW(out, (int) (need_status ? Cmd_Put_Status : Cmd_Put));
        KVW(out, seq);
	if (key.len > MaxKeyLen) {
	    fprintf(stderr, "maxkeylen %d, %.*s\n", key.len, key.len, key.s);
	    exit(EXIT_FAILURE);
	}
        kvwrite_str(out, key);
        // write change
        kvout_reset(kvbuf);
        row_type::kvwrite_change(kvbuf, c);
        kvwrite_str(out, Str(kvbuf->buf, kvbuf->n));
    }
    void sendremove(const Str &key, unsigned int seq) {
        KVW(out, (int)Cmd_Remove);
        KVW(out, seq);
	if (key.len > MaxKeyLen) {
	    fprintf(stderr, "maxkeylen %d, %.*s\n", key.len, key.len, key.s);
	    exit(EXIT_FAILURE);
	}
        kvwrite_str(out, key);
    }
    int recvput(unsigned int *seq, bool gotseq) {
        if(!gotseq && kvread(in, (char*)seq, sizeof(*seq)) != sizeof(*seq))
            return -1;
        return 0;
    }
    int recvputstatus(int *status, unsigned int *seq, bool gotseq) {
        if(!gotseq && kvread(in, (char*)seq, sizeof(*seq)) != sizeof(*seq))
            return -1;
	if (kvread(in, (char *) status, sizeof(*status)) != sizeof(*status))
	    return -1;
        return 0;
    }
    int recvremove(int *status, unsigned int *seq, bool gotseq) {
        if (!gotseq && kvread(in, (char*)seq, sizeof(*seq)) != sizeof(*seq))
            return -1;
	if (kvread(in, (char *) status, sizeof(*status)) != sizeof(*status))
	    return -1;
        return 0;
    }
    void sendscanwhole(int numpairs, const char *key, unsigned int seq) {
        row_type::fields_t f;
        row_type::make_get1_fields(f);
        sendscan(numpairs, key, f, seq);
    }
    void sendscan(int numpairs, const char *key, row_type::fields_t &f,
                  unsigned int seq) {
        KVW(out, (int)Cmd_Scan);
        KVW(out, seq);
        kvwrite_str(out, Str(key, strlen(key)));
        // write fields
        kvout_reset(kvbuf);
        row_type::kvwrite_fields(kvbuf, f);
        kvwrite_str(out, Str(kvbuf->buf, kvbuf->n));
        // write numpairs
        KVW(out, numpairs);
    }
    int recvscan(vector<string> &keys, vector< vector<string> > &rows,
	         unsigned int *seq, bool gotseq) {
        if(!gotseq) {
            unsigned int tmpseq;
            if (!seq)
                seq = &tmpseq;
            if (kvread(in, (char*)seq, sizeof(*seq)) != sizeof(*seq))
                return -1;
        }
        while(1){
            char keybuf[MaxKeyLen];
            int keylen;
            if (kvread_str(in, keybuf, MaxKeyLen, keylen) < 0)
                return -1;
            if (keylen == 0)
                return 0;
            keys.push_back(string(keybuf, keylen));
            vector<string> row;
            if (kvread_row(in, row) < 0)
                return -1;
            rows.push_back(row);
        }
    }
    void cp(int childno) {
	mandatory_assert(childno == 0);
        fprintf(stderr, "asking for a checkpoint\n");
        KVW(out, (int)Cmd_Checkpoint);
        KVW(out, (int)0);
        flush();
        printf("sent\n");
    }

    void flush() {
        kvflush(out);
    }
    void readseq(unsigned int *seq) {
        ssize_t r = kvread(in, (char*)seq, sizeof(*seq));
        mandatory_assert((size_t) r == sizeof(*seq));
    }
    int getPartition() {
	return partition;
    }
    struct kvin *in;
    struct kvout *out;
  private:
    int fdtoclose;
    int partition;
    void handshake() {
        struct kvproto p;
        kvproto_init(p);
        KVW(out, p);
        kvflush(out);
        bool ok;
        int r = KVR(in, ok);
	r = KVR(in, partition);
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
    struct kvout *kvbuf;
    void receive() {
        while (kvcheck(in, 0)) {
            unsigned int seq;
            readseq(&seq);

            KVCallback *cb = reqs.front();
            reqs.pop();
            if(cb->_cmd == Cmd_Get){
                vector<string> row;
                int r = recvget(row, NULL, true);
	        mandatory_assert(r == 0);
                cb->callback_get(0, row);
            } else if(cb->_cmd == Cmd_Put){
	        int r = recvput(NULL, true);
	        mandatory_assert(r >= 0);
                cb->callback_put(0);
            } else if (cb->_cmd == Cmd_Put_Status) {
		int status;
	        int r = recvputstatus(&status, NULL, true);
	        mandatory_assert(r >= 0);
                cb->callback_put(status);
            } else if(cb->_cmd == Cmd_Scan){
                vector<string> keys;
                vector< vector<string> > rows;
                int r = recvscan(keys, rows, NULL, true);
	        mandatory_assert(r == 0);
                mandatory_assert(keys.size() <= cb->_wantedlen);
                cb->callback_scan(0, keys, rows);
            } else if (cb->_cmd == Cmd_Remove) {
		int status;
	        int r = recvremove(&status, NULL, true);
	        mandatory_assert(r >= 0);
                cb->callback_remove(0);
            } else {
                mandatory_assert(0 && "Unknown request");
            }
            cb->decref();
        }
    }

    std::queue<KVCallback *> reqs;
};

#endif
