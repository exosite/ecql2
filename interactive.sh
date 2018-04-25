#!/bin/bash
./rebar3 compile
export CASS_HOST=`./test/ensure_cassandra.sh | tr . ,`
erl -eval "true = code:add_path(\"_build/default/lib/ecql/ebin/\"),ok = application:load(ecql),application:set_env(ecql, replication_factor, 1),application:set_env(ecql, hosts, [{{$CASS_HOST}, 9042}]),{ok, _} = application:ensure_all_started(ecql)."
