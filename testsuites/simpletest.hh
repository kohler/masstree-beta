#ifndef SIMPLETEST_HH
#define SIMPLETEST_HH

#include <unistd.h>
#include <sys/utsname.h>
#include <signal.h>
#include "../clp.h"
#include "../compiler.hh"
#include "../misc.hh"
#include "simpletest_config.hh"
#include "simpletest_thread.hh"
#include "simpletest_runner.hh"


enum { clp_val_normalize = Clp_ValFirstUser, clp_val_suffixdouble };
enum { opt_pin = 1, opt_port, opt_duration,
       opt_test, opt_test_name, opt_threads, opt_trials, opt_quiet, opt_print,
       opt_normalize, opt_limit, opt_notebook, opt_compare, opt_no_run,
       opt_gid, opt_tree_stats, opt_rscale_ncores, opt_cores,
       opt_stats, opt_help};

static const Clp_Option options[] = {
    { "pin", 'p', opt_pin, 0, Clp_Negate },
    { "port", 0, opt_port, Clp_ValInt, 0 },
    { "duration", 'd', opt_duration, Clp_ValDouble, 0 },
    { "limit", 'l', opt_limit, clp_val_suffixdouble, 0 },
    { "normalize", 0, opt_normalize, clp_val_normalize, Clp_Negate },
    { "test", 0, opt_test, Clp_ValString, 0 },
    { "rscale_ncores", 'r', opt_rscale_ncores, Clp_ValInt, 0 },
    { "test-rw1", 0, opt_test_name, 0, 0 },
    { "test-rw2", 0, opt_test_name, 0, 0 },
    { "test-rw3", 0, opt_test_name, 0, 0 },
    { "test-rw4", 0, opt_test_name, 0, 0 },
    { "test-rd1", 0, opt_test_name, 0, 0 },
    { "threads", 'j', opt_threads, Clp_ValInt, 0 },
    { "trials", 'T', opt_trials, Clp_ValInt, 0 },
    { "quiet", 'q', opt_quiet, 0, Clp_Negate },
    { "notebook", 'b', opt_notebook, Clp_ValString, Clp_Negate },
    { "gid", 'g', opt_gid, Clp_ValString, 0 },
    { "compare", 'c', opt_compare, Clp_ValString, 0 },
    { "cores", 0, opt_cores, Clp_ValString, 0 },
    { "no-run", 'n', opt_no_run, 0, 0 },
    { "help", 0, opt_help, 0, 0 }
};

static void help() {
    printf("Masstree-beta mttest\n\
Usage: mttest [-jTHREADS] [OPTIONS] [PARAM=VALUE...] TEST...\n\
       mttest -n -c TESTNAME...\n\
\n\
Options:\n\
  -j, --threads=THREADS    Run with THREADS threads (default %d).\n\
  -p, --pin                Pin each thread to its own core.\n\
  -T, --trials=TRIALS      Run each test TRIALS times.\n\
  -q, --quiet              Do not generate verbose and Gnuplot output.\n\
  -l, --limit=LIMIT        Limit relevant tests to LIMIT operations.\n\
  -d, --duration=TIME      Limit relevant tests to TIME seconds.\n\
  -b, --notebook=FILE      Record JSON results in FILE (notebook-mttest.json).\n\
      --no-notebook        Do not record JSON results.\n\
      --print              Print table after test.\n\
\n\
  -n, --no-run             Do not run new tests.\n\
  -c, --compare=EXPERIMENT Generated plot compares to EXPERIMENT.\n\
\n\
Known TESTs:\n",
           (int) sysconf(_SC_NPROCESSORS_ONLN));
    testrunner_base::print_names(stdout, 5);
    printf("Or say TEST1,TEST2,... to run several tests in sequence\n\
on the same tree.\n");
    exit(0);
}

void runtest(int nthreads, void* (*func)(void*)) {
    std::vector<threadinfo*> tis;
    for (int i = 0; i < nthreads; ++i)
        tis.push_back(threadinfo::make(threadinfo::TI_PROCESS, i));
    signal(SIGALRM, test_timeout);
    for (int i = 0; i < nthreads; ++i) {
        int r = pthread_create(&tis[i]->pthread(), 0, func, tis[i]);
        always_assert(r == 0);
    }
    for (int i = 0; i < nthreads; ++i)
        pthread_join(tis[i]->pthread(), 0);
}


static double level(const std::vector<double> &v, double frac) {
    frac *= v.size() - 1;
    int base = (int) frac;
    if (base == frac)
        return v[base];
    else
        return v[base] * (1 - (frac - base)) + v[base + 1] * (frac - base);
}

static String experiment_test_table_trial(const String &key) {
    const char *l = key.begin(), *r = key.end();
    if (l + 2 < r && l[0] == 'x' && isdigit((unsigned char) l[1])) {
        for (const char *s = l; s != r; ++s)
            if (*s == '/') {
                l = s + 1;
                break;
            }
    }
    return key.substring(l, r);
}

static String experiment_run_test_table(const String &key) {
    const char *l = key.begin(), *r = key.end();
    for (const char *s = r; s != l; --s)
        if (s[-1] == '/') {
            r = s - 1;
            break;
        } else if (!isdigit((unsigned char) s[-1]))
            break;
    return key.substring(l, r);
}

static String experiment_test_table(const String &key) {
    return experiment_run_test_table(experiment_test_table_trial(key));
}

static void run_one_test(int trial, const char *treetype, const char *test,
                         const int *collectorpipe, int nruns);

enum { normtype_none, normtype_pertest, normtype_firsttest };

static void update_labnotebook(String notebook);

void *stat_collector(void *arg) {
    int p = (int) (intptr_t) arg;
    FILE *f = fdopen(p, "r");
    char buf[8192];
    while (fgets(buf, sizeof(buf), f)) {
        Json result = Json::parse(buf);
        if (result && result["table"] && result["test"]) {
            String key = result["test"].to_s() + "/" + result["table"].to_s()
                + "/" + result["trial"].to_s();
            Json &thisex = experiment_stats.get_insert(key);
            thisex[result["thread"].to_i()] = result;
        } else
            fprintf(stderr, "%s\n", buf);
    }
    fclose(f);
    return 0;
}

static String
read_file(FILE *f, const char *name)
{
    lcdf::StringAccum sa;
    while (1) {
        size_t x = fread(sa.reserve(4096), 1, 4096, f);
        if (x != 0)
            sa.adjust_length(x);
        else if (ferror(f)) {
            fprintf(stderr, "%s: %s\n", name, strerror(errno));
            return String::make_stable("???", 3);
        } else
            return sa.take_string();
    }
}


static void
update_labnotebook(String notebook)
{
    FILE *f = (notebook == "-" ? stdin : fopen(notebook.c_str(), "r"));
    String previous_text = (f ? read_file(f, notebook.c_str()) : String());
    if (previous_text.out_of_memory())
        return;
    if (f && f != stdin)
        fclose(f);

    Json nb = Json::parse(previous_text);
    if (previous_text && (!nb.is_object() || !nb["experiments"])) {
        fprintf(stderr, "%s: unexpected contents, not writing new data\n", notebook.c_str());
        return;
    }

    if (!nb)
        nb = Json::make_object();
    if (!nb.get("experiments"))
        nb.set("experiments", Json::make_object());
    if (!nb.get("data"))
        nb.set("data", Json::make_object());

    Json old_data = nb["data"];
    if (!experiment_stats) {
        experiment_stats = old_data;
        return;
    }

    Json xjson;

    FILE *git_info_p = popen("git rev-parse HEAD | tr -d '\n'; git --no-pager diff --exit-code --shortstat HEAD >/dev/null 2>&1 || echo M", "r");
    String git_info = read_file(git_info_p, "<git output>");
    pclose(git_info_p);
    if (git_info)
        xjson.set("git-revision", git_info.trim());

    time_t now = time(0);
    xjson.set("time", String(ctime(&now)).trim());
    if (gid)
        xjson.set("gid", String(gid));

    struct utsname name;
    if (uname(&name) == 0)
        xjson.set("machine", name.nodename);

    xjson.set("cores", udpthreads);

    Json &runs = xjson.get_insert("runs");
    String xname = "x" + String(nb["experiments"].size());
    for (Json::const_iterator it = experiment_stats.begin();
         it != experiment_stats.end(); ++it) {
        String xkey = xname + "/" + it.key();
        runs.push_back(xkey);
        nb["data"][xkey] = it.value();
    }
    xjson.set("runs", runs);

    nb["experiments"][xname] = xjson;

    String new_text = nb.unparse(Json::indent_depth(4).tab_width(2).newline_terminator(true));
    f = (notebook == "-" ? stdout : fopen((notebook + "~").c_str(), "w"));
    if (!f) {
        fprintf(stderr, "%s~: %s\n", notebook.c_str(), strerror(errno));
        return;
    }
    size_t written = fwrite(new_text.data(), 1, new_text.length(), f);
    if (written != size_t(new_text.length())) {
        fprintf(stderr, "%s~: %s\n", notebook.c_str(), strerror(errno));
        fclose(f);
        return;
    }
    if (f != stdout) {
        fclose(f);
        if (rename((notebook + "~").c_str(), notebook.c_str()) != 0)
            fprintf(stderr, "%s: %s\n", notebook.c_str(), strerror(errno));
    }

    fprintf(stderr, "EXPERIMENT %s\n", xname.c_str());
    experiment_stats.merge(old_data);
}
#endif