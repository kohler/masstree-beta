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
#ifndef CLICK_HASHALLOCATOR_HH
#define CLICK_HASHALLOCATOR_HH
#include <stddef.h>
#include <assert.h>
#if HAVE_VALGRIND && HAVE_VALGRIND_MEMCHECK_H
# include <valgrind/memcheck.h>
#endif

class HashAllocator { public:

    HashAllocator(size_t size);
    ~HashAllocator();

    inline void increase_size(size_t new_size) {
	assert(!_free && !_buffer && new_size >= _size);
	_size = new_size;
    }

    inline void *allocate();
    inline void deallocate(void *p);

    void swap(HashAllocator &x);

  private:

    struct link {
	link *next;
    };

    struct buffer {
	buffer *next;
	size_t pos;
	size_t maxpos;
    };

    enum {
	min_buffer_size = 1024,
#if CLICK_LINUXMODULE
	max_buffer_size = 16384,
#else
	max_buffer_size = 1048576,
#endif
	min_nelements = 8
    };

    link *_free;
    buffer *_buffer;
    size_t _size;

    void *hard_allocate();

    HashAllocator(const HashAllocator &x);
    HashAllocator &operator=(const HashAllocator &x);

};


template <size_t size>
class SizedHashAllocator : public HashAllocator { public:

    SizedHashAllocator()
	: HashAllocator(size) {
    }

};


inline void *HashAllocator::allocate()
{
    if (link *l = _free) {
#ifdef VALGRIND_MEMPOOL_ALLOC
	VALGRIND_MEMPOOL_ALLOC(this, l, _size);
	VALGRIND_MAKE_MEM_DEFINED(&l->next, sizeof(l->next));
#endif
	_free = l->next;
#ifdef VALGRIND_MAKE_MEM_DEFINED
	VALGRIND_MAKE_MEM_UNDEFINED(&l->next, sizeof(l->next));
#endif
	return l;
    } else if (_buffer && _buffer->pos < _buffer->maxpos) {
	void *data = reinterpret_cast<char *>(_buffer) + _buffer->pos;
	_buffer->pos += _size;
#ifdef VALGRIND_MEMPOOL_ALLOC
	VALGRIND_MEMPOOL_ALLOC(this, data, _size);
#endif
	return data;
    } else
	return hard_allocate();
}

inline void HashAllocator::deallocate(void *p)
{
    if (p) {
	reinterpret_cast<link *>(p)->next = _free;
	_free = reinterpret_cast<link *>(p);
#ifdef VALGRIND_MEMPOOL_FREE
	VALGRIND_MEMPOOL_FREE(this, p);
#endif
    }
}

#endif
