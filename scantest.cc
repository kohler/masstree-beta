#include "query_masstree.hh"

using namespace Masstree;

kvepoch_t global_log_epoch = 0;
volatile uint64_t globalepoch = 1;     // global epoch, updated by main thread regularly
volatile bool recovering = false; // so don't add log entries, and free old value immediately
kvtimestamp_t initial_timestamp;

int
main(int argc, char *argv[])
{
    if (argc != 2) {
        fprintf(stderr, "usage: %s <callback|iterator>\n", argv[0]);
        return 1;
    }

    threadinfo* ti = threadinfo::make(threadinfo::TI_MAIN, -1);

    if (strcmp(argv[1], "callback") == 0)
        default_table::test(*ti);
    else if (strcmp(argv[1], "iterator") == 0)
        default_table::iterator_test(*ti);
    else {
        fprintf(stderr, "fatal: unknown test\n");
        return 1;
    }

    return 0;
}
