#ifndef SIMPLETEST_CONFIG_HH
#define SIMPLETEST_CONFIG_HH

#include <vector>
#include "../json.hh"
#include "../string.hh"
#include "../nodeversion.hh"
#include "../kvthread.hh"
#include "../misc.hh"

using lcdf::Json;
using lcdf::String;

std::vector<int> cores;
volatile bool timeout[2] = {false, false};
double duration[2] = {10, 0};
int kvtest_first_seed = 31949;
uint64_t test_limit = ~uint64_t(0);
bool quiet = false;
kvepoch_t global_log_epoch = 0;
volatile bool recovering = false; // so don't add log entries, and free old value immediately
kvtimestamp_t initial_timestamp;
static const char *threadcounter_names[(int) tc_max];

static Json test_param;
static const char *gid = NULL;
static int udpthreads = 0;
static int tcpthreads = 0;

static bool pinthreads = false;
static nodeversion32 global_epoch_lock(false);
volatile mrcu_epoch_type globalepoch = 1;     // global epoch, updated by main thread regularly
volatile mrcu_epoch_type active_epoch = 1;
static int port = 2117;
static int rscale_ncores = 0;
static volatile int kvtest_printing;

static const char *current_test_name;
static int current_trial;
static FILE *test_output_file;
static pthread_mutex_t subtest_mutex;
static pthread_cond_t subtest_cond;

static const char * const kvstats_name[] = {
    "ops_per_sec", "puts_per_sec", "gets_per_sec", "scans_per_sec"
};
static Json experiment_stats;

void test_timeout(int) {
  size_t n;
  for (n = 0; n < arraysize(timeout) && timeout[n]; ++n)
    /* do nothing */;
  if (n < arraysize(timeout)) {
    timeout[n] = true;
    if (n + 1 < arraysize(timeout) && duration[n + 1])
      xalarm(duration[n + 1]);
  }
}

void set_global_epoch(mrcu_epoch_type e) {
  global_epoch_lock.lock();
  if (mrcu_signed_epoch_type(e - globalepoch) > 0) {
    globalepoch = e;
    active_epoch = threadinfo::min_active_epoch();
  }
  global_epoch_lock.unlock();
}
#endif