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
/*
 * This file defines configuration used by both kvd and kv clients.
 */
#ifndef SHARED_CONFIG_HH
#define SHARED_CONFIG_HH 1
#include "config.h"
#include "compiler.hh"

enum { MaxKeyLen = KVDB_MAX_KEY_LEN }; // Maximum length of a key
enum { MaxRowLen = KVDB_MAX_ROW_LEN }; // Maximum length of a row

typedef uint64_t kvtimestamp_t;


struct kvr_timed_array;
struct kvr_timed_array_ver;
template <typename O> struct kvr_timed_bag;
struct kvr_timed_str;

enum rowtype_id {
    RowType_Str = 0,
    RowType_Array = 1,
    RowType_ArrayVer = 2,
    RowType_Bag = 3
};

#if KVDB_ROW_TYPE_ARRAY
# define KVDB_ROW_TYPE_INCLUDE "kvr_timed_array.hh"
# define KVDB_ROW_TYPE_ID RowType_Array
typedef kvr_timed_array row_type;
#elif KVDB_ROW_TYPE_ARRAY_VER
# define KVDB_ROW_TYPE_INCLUDE "kvr_timed_array_ver.hh"
# define KVDB_ROW_TYPE_ID RowType_ArrayVer
typedef kvr_timed_array_ver row_type;
#elif KVDB_ROW_TYPE_BAG
# define KVDB_ROW_TYPE_INCLUDE "kvr_timed_bag.hh"
# define KVDB_ROW_TYPE_ID RowType_Bag
typedef kvr_timed_bag<uint16_t> row_type;
#else
# define KVDB_ROW_TYPE_INCLUDE "kvr_timed_str.hh"
# define KVDB_ROW_TYPE_ID RowType_Str
typedef kvr_timed_str row_type;
#endif

#endif
