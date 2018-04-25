-module(ecql_batch_unit).
-export([tests/0]).
-include_lib("eunit/include/eunit.hrl").

% Testing batch features

tests() ->
  {setup, fun start/0, fun stop/1, [
    {"Large batch", {timeout, 60, fun large_batch_tests/0}}
  ]}
.

start() ->
  {ok, _Apps} = application:ensure_all_started(ecql),
  Table = "CREATE TABLE IF NOT EXISTS batch_test (a int PRIMARY KEY, b text);",
  ecql:execute(Table)
.

stop(_) ->
  %ecql:execute("DROP TABLE batch_test"),
  ok
.

%% TESTS START HERE
large_batch_tests() ->
  Str = "roundandroundandroundandroundand",
  Values = [[N, string:copies(Str, N)] || N <- lists:seq(1, 1000, 5)],
  ok = ecql:execute_batch("INSERT INTO batch_test (a, b) VALUES(?, ?)", Values)
.


