#!/bin/bash

echo
echo "Don't forget to fix your cpu speed! e.g. 'sudo cpufreq-set -f 1700000'"
echo
# sudo cpufreq-set -f 1700000

erl -make
str=ok
for file in ecql_stream*; do
  mod=${file%.*}
  str="$str,encoding_test:test($mod,100)"
done
erl -noshell -eval "$str,init:stop()."
