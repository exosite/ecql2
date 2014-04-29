-module(ecql_tests).
-include_lib("eunit/include/eunit.hrl").


setup() ->
   SaslOpt = [{sasl_error_logger, {file, "sasl.log"}}]
  ,io:format("sasl~n")
  ,start_app(sasl, SaslOpt)
  ,{ok, Ip} = file:read_file("./cassandra.ip")
  ,{ok, Host} = inet_parse:address(string:chomp(binary_to_list(Ip)))
  ,EcqlOpt = [
    {replication_strategy, "SimpleStrategy"},
    {replication_factor, 1},
    {keyspace, "test"},
    {hosts, [{Host, 9042}]}
  ]
  ,start_app(ecql, EcqlOpt)
.

cleanup(_) ->
  ok
.

ecql_test_() ->
  {setup, fun setup/0, fun cleanup/1, {generator, fun tests/0}}
.

tests() ->
  [
    %~ {generator, fun ecql_mnesia_unit:tests/0},
    {generator, fun ecql_prepared_unit:tests/0}
    %~ {generator, fun ecql_batch_unit:tests/0},
    %~ {generator, fun ecql_types_unit:tests/0}
  ]
.

start_app(App, Options) ->
   ok = application:load(App)
  ,[application:set_env(App, Key, Value) || {Key, Value} <- Options]
  ,{Ret, _} = application:ensure_all_started(App)
  ,Ret == ok orelse Ret == already_started
.

