# MASSTREE-BETA RELEASE 0.1 #

This is the README file for the source release for Masstree - a fast key-value
store for multicore. The purpose of the document is to describe the code
layout, and necessary information for you to run Masstree and interpreting the
result.  More recent information may be available at
[Masstree website](http://pdos.csail.mit.edu/masstree).

##CONTENTS##

* `MTDIR`                     This directory
* `MTDIR/doc`               Masstree algorithm specification

##Install##
Masstree is tested on Debian, Ubuntu and Mac OS X. Install Masstree as:

    $ ./configure
    $ make

By default, Masstree links with glibc's malloc. You can also configure Masstree
to link with another memory allocator as:

    ./configure --with-malloc=<jemalloc|flow|tcmalloc>`.

Flow is our re-implementation of
[Streamflow](http://people.cs.vt.edu/~scschnei/streamflow/) allocator, and may
be open-sourced in future.

See `./configure --help` for more configure options.


##Test Masstree in a single process##

The quickiest and simplest way to try out Masstree is the `./mttest` program.
(**Note that this test doesn't involve disk or network overhead.**)

<pre>
$ ./mttest
1/1 rw1/m
0: now getting
1: now getting
0: {"table":"mb","test":"rw1","trial":0,"thread":0,"puts":13243551,"puts_per_sec":1324492.05531,"gets":13243551,"gets_per_sec":1497267.13928,"ops":26487102,"ops_per_sec":1405590.1258}
1: {"table":"mb","test":"rw1","trial":0,"thread":1,"puts":13242601,"puts_per_sec":1324397.45602,"gets":13242601,"gets_per_sec":1481151.35726,"ops":26485202,"ops_per_sec":1398395.26601}
EXPERIMENT x0
set title "mat (2 cores)"
set terminal png
set xrange [0.96:1.44]
set xtics rotate ("rw1/mb" 1, "rw1/mb" 1.2, "rw1/mb" 1.4)
set key top left Left reverse
set ylabel "count, normalized per test"
plot  '-' using 1:($3/$6):($2/$6):($5/$6):($4/$6) with candlesticks lt 1 title 'ops_per_sec', \
 '-' using 1:($2/$3):($2/$3):($2/$3):($2/$3) with candlesticks lt 1 notitle, \
 '-' using 1:($3/$6):($2/$6):($5/$6):($4/$6) with candlesticks lt 2 title 'puts_per_sec', \
 '-' using 1:($2/$3):($2/$3):($2/$3):($2/$3) with candlesticks lt 2 notitle, \
 '-' using 1:($3/$6):($2/$6):($5/$6):($4/$6) with candlesticks lt 3 title 'gets_per_sec', \
 '-' using 1:($2/$3):($2/$3):($2/$3):($2/$3) with candlesticks lt 3 notitle
1 1398395.26601 1400193.98096 1403791.41085 1405590.1258 1401992.6959
e
1 1401992.6959 1401992.6959
e
1.2 1324397.45602 1324421.10584 1324468.40549 1324492.05531 1324444.75567
e
1.2 1324444.75567 1324444.75567
e
1.4 1481151.35726 1485180.30276 1493238.19378 1497267.13928 1489209.24827
e
1.4 1489209.24827 1489209.24827
e
</pre>

The test starts a process which hosts a Masstree, generates and execute queries
over the tree. It uses all available cores (two in the above example). The test
lasts for 20 seconds. It populates the key-value store with `put` queries
during first 10 seconds, and then issues `get` queries over the tree during the
second10 seconds, i.e. the getting phase. See `kvtest_rw1_seed` in `mttest.hh`
for more details about the workload and other workloads that `mttest` supports.

The output summarizes the throughput of each core. The `1/1 rw1/m` line says
that `mttest` is running the first trial (out of one trials), of the `rw1`
workload using Masstree (`m` for short) as the internal data structure.

The rest of the result mainly consists of two parts. First is the per-core
throughput summary, as indicated by `0: {"table":"mb","test":"rw1",...}`. The
rest is the gnuplot source that plot the median per-core throughput. If you
plot it, each candlestick has five points, each represents the
min,20%,50%,70%,max of the corresponding metric among all threads.

##Output format##

`mttest` can also write the output as JSON into file for further analysis. For
example, `./mttest -b notebook.json` will create `notebook.json` containing:

<pre>
{
  "experiments":{
    "x0":{
      "git-revision":"673994c43d58d46f4ebf3f7d4e1fce19074594cb",
      "time":"Wed Oct 24 14:54:39 2012",
      "machine":"mat",
      "cores":2,
      "runs":["x0\/rw1\/mb\/0"]
    }
  },
  "data":{
    "x0\/rw1\/mb\/0":[
      {
        "table":"mb",
        "test":"rw1",
        "trial":0,
        "thread":0,
        "puts":13243551,
        "puts_per_sec":1324492.05531,
        "gets":13243551,
        "gets_per_sec":1497267.13928,
        "ops":26487102,
        "ops_per_sec":1405590.1258
      },
      {
        "table":"mb",
        "test":"rw1",
        "trial":0,
        "thread":1,
        "puts":13242601,
        "puts_per_sec":1324397.45602,
        "gets":13242601,
        "gets_per_sec":1481151.35726,
        "ops":26485202,
        "ops_per_sec":1398395.26601
      }
    ]
  }
}
</pre>

##Test Masstree with `mtclient`##

`mtclient` supports almost the same set of workloads that `mttest` does, but it
sends queries to the Masstree deamon over network.

To start Masstree daemon, run:

<pre>
$ ./mtd --logdir=[LOG_DIRS]  --ckdir=[CHECKPOINT_DIRS]
mb, Bag, pin-threads disabled, logging enabled
no ./kvd-ckp-gen
no ./kvd-ckp-0-0
no ./kvd-ckp-0-1
2 udp threads
2 tcp threads
</pre>

`LOG_DIRS` is a comma-separated list of directories storing Masstree
logs, and `CHECKPOINT_DIRS` is a comm-separated list of directory storing
Masstree checkpoints.

To run `rw1` workload with `mtclient` on the same machine as `mtd`, run:

<pre>
$ ./mtclient -s 127.0.0.1 rw1
tcp, w 500, test rw1, children 2
0 now getting
1 now getting
0 total 7632001 763284 put/s 1263548 get/s
1 total 7612501 761423 put/s 1259847 get/s
{"puts":7632001,"puts_per_sec":763284.211682,"gets":7632001,"gets_per_sec":1263548.30195,"ops":15264002,"ops_per_sec":951678.506329}
{"puts":7612501,"puts_per_sec":761423.014367,"gets":7612501,"gets_per_sec":1259847.22076,"ops":15225002,"ops_per_sec":949182.006246}
total 30489004
puts: n 2, total 15244502, average 7622251, min 7612501, max 7632001, stddev 13789
gets: n 2, total 15244502, average 7622251, min 7612501, max 7632001, stddev 13789
puts/s: n 2, total 1524707, average 762354, min 761423, max 763284, stddev 1316
gets/s: n 2, total 2523396, average 1261698, min 1259847, max 1263548, stddev 2617
</pre>


##Factor Analysis##

We have included the script that allows you to reproduce the factor analysis
graph in Masstree paper. Run:

    $ ./script/factor.py

Then you can use `./script/factorgraph.py` to convert the result into plottable
gnuplot data.
