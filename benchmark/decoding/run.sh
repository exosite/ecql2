#!/bin/bash

echo
echo "Don't forget to fix your cpu speed! e.g. 'sudo cpufreq-set -f 1700000'"
echo
# sudo cpufreq-set -f 1700000

rep=100

erl -make
str=ok
for file in ecql_decoding_?_*erl; do
#for file in ecql_decoding_1_*erl; do
  mod=${file%.*}
  str="$str,code:load_file($mod)"
  for i in 1, 2, 3; do
  #for i in 1, 2, 3, 4, 5, 6, 7, 8, 9, 10; do
    str="$str,cqltest:test($mod, $rep)"
  done
done
#echo erl -noshell -eval "$str,init:stop()."
erl -noshell -eval "$str,init:stop()."
