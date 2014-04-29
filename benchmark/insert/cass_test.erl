-module(cass_test).
-export([init/0]).
-export([test/2]).
-export([start_app/2]).

init() ->
   SaslOpt = [{sasl_error_logger, {file, "sasl.log"}}]
  ,start_app(sasl, SaslOpt)
  ,{ok, Host} = inet_parse:address(os:getenv("CASS_HOST", "127.0.0.1"))
  ,EcqlOpt = [
    {replication_strategy, "SimpleStrategy"},
    {replication_factor, 1},
    {keyspace, "test"},
    {hosts, [{Host, 9042}]}
  ]
  ,start_app(ecql, EcqlOpt)
.

test(Module, Inserts) ->
   ecql:execute("TRUNCATE TABLE record_name")
  ,Module:init(Inserts)
  ,R = do_test(Module, Inserts)
  ,Rows = [[I, I] || I <- lists:seq(1, Inserts)]
  ,{_, Unsorted} = ecql:select("SELECT * FROM record_name LIMIT ?", [Inserts])
  ,io:format("~p ~p inserts: ~p seconds ~p/~p records~n", [Module, Inserts, R/1000000, length(Rows), length(Unsorted)])
.

do_test(Module, Inserts) ->
   {Time, _} = timer:tc(Module, test, [Inserts])
  ,Time
.

start_app(App, Options) ->
   ok = application:load(App)
  ,[application:set_env(App, Key, Value) || {Key, Value} <- Options]
  ,{Ret, _} = application:ensure_all_started(App)
  ,Ret == ok orelse Ret == already_started
.

