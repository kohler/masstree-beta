#ifndef SIMPLETEST_THREAD_HH
#define SIMPLETEST_THREAD_HH
#include "simpletest_config.hh"
#include "simpletest_runner.hh"
#include "../misc.hh"

enum {
  test_thread_initialize = 1,
  test_thread_destroy = 2,
};

template <typename C>
struct test_thread {
    using T = typename C::table_type;
    test_thread(threadinfo* ti) {
        client_.set_table(table_, ti);
    }
    static void setup(threadinfo* ti, int action) {
        if (action == test_thread_initialize) {
            assert(!table_);
            table_ = new T;
            table_->initialize(*ti);
        } else if (action == test_thread_destroy) {
            assert(table_);
            delete table_;
            table_ = 0;
        }
    }
    static void* go(void* x) {
        threadinfo* ti = reinterpret_cast<threadinfo*>(x);
        ti->pthread() = pthread_self();
        assert(table_);
#if __linux__
        if (pinthreads) {
            cpu_set_t cs;
            CPU_ZERO(&cs);
            CPU_SET(cores[ti->index()], &cs);
            int r = sched_setaffinity(0, sizeof(cs), &cs);
            always_assert(r == 0);
        }
#else
        always_assert(!pinthreads && "pinthreads not supported\n");
#endif

        test_thread<C> tt(ti);
        if (fetch_and_add(&active_threads_, 1) == 0)
            tt.ready_timeouts();
        String test = ::current_test_name;
        int subtestno = 0;
        for (int pos = 0; pos < test.length(); ) {
            int comma = test.find_left(',', pos);
            comma = (comma < 0 ? test.length() : comma);
            String subtest = test.substr(pos, comma - pos), tname;
            testrunner<C>* tr = testrunner<C>::find(subtest, C::tname());
            tname = (subtest == test ? subtest : test + String("@") + String(subtestno));
            tt.client_.reset(tname, ::current_trial);
            if (tr)
                tr->run(tt.client_);
            else
                tt.client_.fail("unknown test %s", subtest.c_str());
            if (comma == test.length())
                break;
            pthread_mutex_lock(&subtest_mutex);
            if (fetch_and_add(&active_threads_, -1) == 1) {
                pthread_cond_broadcast(&subtest_cond);
                tt.ready_timeouts();
            } else
                pthread_cond_wait(&subtest_cond, &subtest_mutex);
            fprintf(test_output_file, "%s\n", tt.client_.report_.unparse().c_str());
            pthread_mutex_unlock(&subtest_mutex);
            fetch_and_add(&active_threads_, 1);
            pos = comma + 1;
            ++subtestno;
        }
        int at = fetch_and_add(&active_threads_, -1);
        fprintf(test_output_file, "%s\n", tt.client_.report_.unparse().c_str());
        return 0;
    }
    void ready_timeouts() {
        for (size_t i = 0; i < arraysize(timeout); ++i) {
            timeout[i] = false;
        }
        if (duration[0]) {
            xalarm(duration[0]);
        }
    }
    static typename C::table_type* table_;
    static unsigned active_threads_;
    C client_;
};
template <typename C> typename C::table_type* test_thread<C>::table_;
template <typename C> unsigned test_thread<C>::active_threads_;

#endif