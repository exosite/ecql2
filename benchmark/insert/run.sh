#!/bin/bash

export CASS_HOST=`../../test/ensure_cassandra.sh`

echo
echo "Don't forget to fix your cpu speed! e.g. 'sudo cpufreq-set -f 1700000'"
echo
# sudo cpufreq-set -f 1700000

erl -make
str='true=code:add_path("../../_build/default/lib/ecql/ebin"),application:start(cass_test),cass_test:init()'
for file in ecql_*; do
  mod=${file%.*}
  str="$str,cass_test:test($mod,50000)"
done
echo erl -noshell -eval "$str,init:stop()."
erl -noshell -eval "$str,init:stop()." | grep insert
#erl -noshell -eval "$str,init:stop()."
