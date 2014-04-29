Internal Performance Benchmarks
============================

This directory contains benchmarks for critical performance sections inside
the driver itself. These are implementation variants of the same functions
to benchmark which variant yields the best performance. These benchmarks
server as reference to how the current implementations were evaluated and
provide means to re-evaluate alternative on new implementations of the 
erlang VM or cassamdra.

Usage
-----

When running these machines on a notebook or other computer with power save
function please ensure to disable automatic performance throttling before doing
any measurements.

```
cd <testname>
./run.sh
```

E.g. to run the insert tests against cassandra

```
cd insert/
./run.sh
```

Test
------------------

* insert - These are insert test to check different insert strategies performance against cassandra. Based on these results the ecql:bulk functions are implemented for highest performance.

* encoding - Tests different encoder implementations to check erlang vm performance. Newer VM versions might change the balance here.

* decoding - Similiar to encoding tests different decoder implementations. Since decoding proved to be a bottleneck this benchmark also contains the current c-version.

Examples
--------

```
$ cd decoding; ./run.sh 

Don't forget to fix your cpu speed! e.g. 'sudo cpufreq-set -f 1700000'

ecql_decoding_1_4 100 iterations: 11737.2
ecql_decoding_1_4 100 iterations: 12284.79
ecql_decoding_1_4 100 iterations: 11750.34
ecql_decoding_1_5 100 iterations: 27302.38
ecql_decoding_1_5 100 iterations: 27636.9
ecql_decoding_1_5 100 iterations: 27651.49
ecql_decoding_2_0 100 iterations: 1953.2
ecql_decoding_2_0 100 iterations: 1927.55
ecql_decoding_2_0 100 iterations: 1939.47
ecql_decoding_3_8 100 iterations: 25266.07
ecql_decoding_3_8 100 iterations: 25090.1
ecql_decoding_3_8 100 iterations: 25023.54
ecql_decoding_5_5 100 iterations: 16464.77
ecql_decoding_5_5 100 iterations: 16551.67
ecql_decoding_5_5 100 iterations: 16440.36
ecql_decoding_6_0 100 iterations: 37549.13
ecql_decoding_6_0 100 iterations: 37863.17
ecql_decoding_6_0 100 iterations: 37914.73
ecql_decoding_7_0 100 iterations: 33968.19
ecql_decoding_7_0 100 iterations: 34032.98
ecql_decoding_7_0 100 iterations: 34420.23
ecql_decoding_9_8 100 iterations: 37736.04
ecql_decoding_9_8 100 iterations: 37330.04
ecql_decoding_9_8 100 iterations: 37128.9
```

```
$ cd encoding; ./run.sh 

Don't forget to fix your cpu speed! e.g. 'sudo cpufreq-set -f 1700000'

ecql_stream_1200 100 iterations: 1092.01 microseconds per call
ecql_stream_1200 100 iterations: 1090.81 microseconds per call
ecql_stream_2800 100 iterations: 2297.56 microseconds per call
ecql_stream_2800 100 iterations: 2414.1 microseconds per call
ecql_stream_3000 100 iterations: 2452.87 microseconds per call
ecql_stream_3000 100 iterations: 2395.35 microseconds per call
ecql_stream_3500 100 iterations: 2951.65 microseconds per call
ecql_stream_3500 100 iterations: 3001.88 microseconds per call
ecql_stream_3800 100 iterations: 3029.69 microseconds per call
ecql_stream_3800 100 iterations: 3105.84 microseconds per call
```

```
$ cd insert/; ./run.sh 

Don't forget to fix your cpu speed! e.g. 'sudo cpufreq-set -f 1700000'

ecql_async 50000 inserts: 1.514355 seconds
ecql_async 50000 inserts: 1.524418 seconds
ecql_batch_pure2 50000 inserts: 0.620239 seconds
ecql_batch_pure2 50000 inserts: 0.690847 seconds
ecql_batch_pure 50000 inserts: 0.84848 seconds
ecql_batch_pure 50000 inserts: 0.813553 seconds
ecql_logged_batch 50000 inserts: 0.760976 seconds
ecql_logged_batch 50000 inserts: 0.986544 seconds
ecql_unlogged_batch 50000 inserts: 0.89411 seconds
ecql_unlogged_batch 50000 inserts: 0.795647 seconds
```
