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
#include "kvtable.hh"
#include "compiler.hh"
#include <string.h>
#include <assert.h>

namespace {
struct node {
    const char *name;
    kvtable_factory *factory;
    node *next;
};
node *nodelist;
static unsigned lock;
}

void
kvtable_factory::add(const char *name, kvtable_factory *factory)
{
    test_and_set_acquire(&lock);
    assert(!find(name));
    node *n = new node;
    n->name = name;
    n->factory = factory;
    n->next = nodelist;
    nodelist = n;
    test_and_set_release(&lock);
}

kvtable_factory *
kvtable_factory::find(const char *name)
{
    for (node *n = nodelist; n; n = n->next)
	if (strcasecmp(name, n->name) == 0)
	    return n->factory;
    return 0;
}

kvtable *
kvtable_factory::create(const char *name)
{
    if (kvtable_factory *factory = find(name))
        return factory->create();
    else
        return 0;
}

std::vector<const char *>
kvtable_factory::table_types()
{
    std::vector<const char *> names;
    for (node *n = nodelist; n; n = n->next)
        names.push_back(n->name);
    return names;
}
