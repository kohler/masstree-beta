#include "simpletest.hh"
#include "simpletest_config.hh"

#include <ctype.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#if HAVE_NUMA_H
#include <numa.h>
#endif
#if HAVE_SYS_EPOLL_H
#include <sys/epoll.h>
#endif
#if HAVE_EXECINFO_H
#include <execinfo.h>
#endif
#if __linux__
#include <asm-generic/mman.h>
#endif
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <pthread.h>
#include <string.h>
#ifdef __linux__
#include <malloc.h>
#endif
#include <algorithm>
#include <numeric>

#include "masstree_test.hh"
#include "binarytree_test.hh"
#include "fourtree_test.hh"
#include "avltree_test.hh"

using masstree_test_thread = test_thread<masstree_test_client>;
using binarytree_test_thread = test_thread<binarytree_test_client>;
using fourtree_test_thread = test_thread<fourtree_test_client>;
using avltree_test_thread = test_thread<avltree_test_client>;

static struct {
    const char *treetype;
    void* (*go_func)(void*);
    void (*setup_func)(threadinfo*, int);
} test_thread_map[] = {
    { "masstree", masstree_test_thread::go, masstree_test_thread::setup },
    { "binarytree", binarytree_test_thread::go, binarytree_test_thread::setup },
    { "fourtree", fourtree_test_thread::go, fourtree_test_thread::setup },
    { "avltree", avltree_test_thread ::go, avltree_test_thread ::setup }
};

static void run_one_test_body(int trial, const char *treetype, const char *test) {
    threadinfo *main_ti = threadinfo::make(threadinfo::TI_MAIN, -1);
    main_ti->pthread() = pthread_self();
    globalepoch = active_epoch = timestamp() >> 16;
    for (int i = 0; i < (int) arraysize(test_thread_map); ++i)
        if (strcmp(test_thread_map[i].treetype, treetype) == 0) {
            current_test_name = test;
            current_trial = trial;
            test_thread_map[i].setup_func(main_ti, test_thread_initialize);
            runtest(tcpthreads, test_thread_map[i].go_func);
            test_thread_map[i].setup_func(main_ti, test_thread_destroy);
            break;
        }
}

static void run_one_test(int trial, const char *treetype, const char *test,
                         const int *collectorpipe, int nruns) {
    if (nruns == 1)
        run_one_test_body(trial, treetype, test);
    else {
        pid_t c = fork();
        if (c == 0) {
            close(collectorpipe[0]);
            run_one_test_body(trial, treetype, test);
            exit(0);
        } else
            while (waitpid(c, 0, 0) == -1 && errno == EINTR)
                /* loop */;
    }
}

#if HAVE_EXECINFO_H
static const int abortable_signals[] = {
    SIGSEGV, SIGBUS, SIGILL, SIGABRT, SIGFPE
};

static void abortable_signal_handler(int) {
    // reset signals so if a signal recurs, we exit
    for (const int* it = abortable_signals;
         it != abortable_signals + arraysize(abortable_signals); ++it)
        signal(*it, SIG_DFL);
    // dump backtrace to standard error
    void* return_addrs[50];
    int n = backtrace(return_addrs, arraysize(return_addrs));
    backtrace_symbols_fd(return_addrs, n, STDERR_FILENO);
    // re-abort
    abort();
}
#endif


int
main(int argc, char *argv[])
{
    int ret, ntrials = 1, normtype = normtype_pertest, firstcore = -1, corestride = 1;
    std::vector<const char *> tests, treetypes;
    std::vector<String> comparisons;
    const char *notebook = "notebook-mttest.json";
    tcpthreads = udpthreads = sysconf(_SC_NPROCESSORS_ONLN);

    Clp_Parser *clp = Clp_NewParser(argc, argv, (int) arraysize(options), options);
    Clp_AddStringListType(clp, clp_val_normalize, 0,
                          "none", (int) normtype_none,
                          "pertest", (int) normtype_pertest,
                          "test", (int) normtype_pertest,
                          "firsttest", (int) normtype_firsttest,
                          (const char *) 0);
    Clp_AddType(clp, clp_val_suffixdouble, Clp_DisallowOptions, clp_parse_suffixdouble, 0);
    int opt;
    while ((opt = Clp_Next(clp)) != Clp_Done) {
        switch (opt) {
        case opt_pin:
            pinthreads = !clp->negated;
            break;
        case opt_threads:
            tcpthreads = udpthreads = clp->val.i;
            break;
        case opt_trials:
            ntrials = clp->val.i;
            break;
        case opt_quiet:
            quiet = !clp->negated;
            break;
        case opt_rscale_ncores:
            rscale_ncores = clp->val.i;
            break;
        case opt_port:
            port = clp->val.i;
            break;
        case opt_duration:
            duration[0] = clp->val.d;
            break;
        case opt_limit:
            test_limit = uint64_t(clp->val.d);
            break;
        case opt_test:
            tests.push_back(clp->vstr);
            break;
        case opt_test_name:
            tests.push_back(clp->option->long_name + 5);
            break;
        case opt_normalize:
            normtype = clp->negated ? normtype_none : clp->val.i;
            break;
        case opt_gid:
            gid = clp->vstr;
            break;
        case opt_notebook:
            if (clp->negated)
                notebook = 0;
            else if (clp->have_val)
                notebook = clp->vstr;
            else
                notebook = "notebook-mttest.json";
            break;
        case opt_compare:
            comparisons.push_back(clp->vstr);
            break;
        case opt_no_run:
            ntrials = 0;
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
        case opt_help:
            help();
            break;
        case Clp_NotOption:
            // check for parameter setting
            if (const char* eqchr = strchr(clp->vstr, '=')) {
                Json& param = test_param[String(clp->vstr, eqchr)];
                const char* end_vstr = clp->vstr + strlen(clp->vstr);
                if (param.assign_parse(eqchr + 1, end_vstr)) {
                    // OK, param was valid JSON
                } else if (eqchr[1] != 0) {
                    param = String(eqchr + 1, end_vstr);
                } else {
                    param = Json();
                }
            } else {
                // otherwise, tree or test
                bool is_treetype = false;
                for (int i = 0; i < (int) arraysize(test_thread_map) && !is_treetype; ++i) {
                    is_treetype = (strcmp(test_thread_map[i].treetype, clp->vstr) == 0);
                }
                (is_treetype ? treetypes.push_back(clp->vstr) : tests.push_back(clp->vstr));
            }
            break;
        default:
            fprintf(stderr, "Usage: mttest [-jN] TESTS...\n\
Try 'mttest --help' for options.\n");
            exit(EXIT_FAILURE);
        }
    }
    Clp_DeleteParser(clp);
    if (firstcore < 0)
        firstcore = cores.size() ? cores.back() + 1 : 0;
    for (; (int) cores.size() < udpthreads; firstcore += corestride)
        cores.push_back(firstcore);

#if PMC_ENABLED
    always_assert(pinthreads && "Using performance counter requires pinning threads to cores!");
#endif
#if MEMSTATS && HAVE_NUMA_H && HAVE_LIBNUMA
    if (numa_available() != -1)
        for (int i = 0; i <= numa_max_node(); i++) {
            numa.push_back(mttest_numainfo());
            numa.back().size = numa_node_size64(i, &numa.back().free);
        }
#endif
#if HAVE_EXECINFO_H
    for (const int* it = abortable_signals;
         it != abortable_signals + arraysize(abortable_signals); ++it)
        signal(*it, abortable_signal_handler);
#endif

    if (treetypes.empty())
        treetypes.push_back("masstree");
    if (tests.empty())
        tests.push_back("rw1");

    pthread_mutex_init(&subtest_mutex, 0);
    pthread_cond_init(&subtest_cond, 0);

    // pipe for them to write back to us
    int p[2];
    ret = pipe(p);
    always_assert(ret == 0);
    test_output_file = fdopen(p[1], "w");

    pthread_t collector;
    ret = pthread_create(&collector, 0, stat_collector, (void *) (intptr_t) p[0]);
    always_assert(ret == 0);
    initial_timestamp = timestamp();

    // run tests
    int nruns = ntrials * (int) tests.size() * (int) treetypes.size();
    std::vector<int> runlist(nruns, 0);
    for (int i = 0; i < nruns; ++i)
        runlist[i] = i;

    for (int counter = 0; counter < nruns; ++counter) {
        int x = random() % runlist.size();
        int run = runlist[x];
        runlist[x] = runlist.back();
        runlist.pop_back();

        int trial = run % ntrials;
        run /= ntrials;
        int t = run % tests.size();
        run /= tests.size();
        int tt = run;

        fprintf(stderr, "%d/%u %s/%s%s", counter + 1, (int) (ntrials * tests.size() * treetypes.size()),
                tests[t], treetypes[tt], quiet ? "      " : "\n");

        run_one_test(trial, treetypes[tt], tests[t], p, nruns);
        struct timeval delay;
        delay.tv_sec = 0;
        delay.tv_usec = 250000;
        (void) select(0, 0, 0, 0, &delay);

        if (quiet)
            fprintf(stderr, "\r%60s\r", "");
    }

    fclose(test_output_file);
    pthread_join(collector, 0);

    // update lab notebook
    if (notebook)
        update_labnotebook(notebook);

    return 0;
}
