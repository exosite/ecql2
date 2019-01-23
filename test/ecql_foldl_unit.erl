-module(ecql_foldl_unit).
-export([tests/0]).
-include_lib("eunit/include/eunit.hrl").

% Testing batch features

tests() ->
  {setup, fun start/0, fun stop/1, [
    {"foldl", {timeout, 60, fun foldl/0}},
    {"foldl_in", {timeout, 60, fun foldl_in/0}}
  ]}
.

start() ->
  {ok, _Apps} = application:ensure_all_started(ecql),
  Table = "CREATE TABLE IF NOT EXISTS foldl_test (a int PRIMARY KEY, b text);",
  ok = ecql:execute(Table),
  Values = [[N, "Value"] || N <- lists:seq(1, 10000)],
  ok = ecql:execute_batch("INSERT INTO foldl_test (a, b) VALUES(?, ?)", Values)
.

stop(_) ->
  %ecql:execute("DROP TABLE foldl_test"),
  ok
.

%% TESTS START HERE
foldl() ->
  EcqlProdSum = ecql:foldl(
    fun([Idx], Acc) ->
      Acc + (Idx*Idx)
    end,
    0,
    "SELECT a FROM foldl_test"
  ),
  TestProdSum = lists:foldl(
    fun(Idx, Acc) ->
      Acc + (Idx*Idx)
    end,
    0,
    seq()
  ),
  EcqlProdSum = TestProdSum
.

foldl_in() ->
  EcqlProdSum = ecql:foldl(
    fun([Idx], Acc) ->
      case Idx of
        2001 -> foldl() + Acc + (Idx * Idx);
        _ -> Acc + (Idx * Idx)
      end
    end,
    0,
    "SELECT a FROM foldl_test"
  ),
  TestProdSum = lists:foldl(
    fun(Idx, Acc) ->
      Acc + (Idx*Idx)
    end,
    0,
    seq()
  ),
  EcqlProdSum = 2 * TestProdSum
.



seq() -> lists:seq(1, 10000).