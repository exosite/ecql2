-module(ecql_prepared_unit).
-export([tests/0]).
-include_lib("eunit/include/eunit.hrl").

% Testing prepared statements

tests() ->
  {setup, fun start/0, fun stop/1, [
    {"Statement invalidation", fun invalid_statement/0},
    {"Statement invalidation", fun invalid_statement_async/0}
  ]}
.

start() ->
  {ok, _Apps} = application:ensure_all_started(ecql),
  cql("CREATE TABLE IF NOT EXISTS prepared_test (a int PRIMARY KEY, b text);")
.

stop(_) ->
  cql("DROP TABLE prepared_test;")
.

%% TESTS START HERE
invalid_statement() ->
  ok = insert("test1"),
  <<"test1">> = read(),

  cql("DROP TABLE prepared_test;"),
  cql("CREATE TABLE IF NOT EXISTS prepared_test (a int PRIMARY KEY, b text);"),

  ok = insert("test2"),
  <<"test2">> = read(),

  ok = insert("test3"),
  <<"test3">> = read()
.

insert(Value) ->
  ecql:execute("INSERT INTO prepared_test (a, b) VALUES(?, ?)", [1, Value])
.

invalid_statement_async() ->
  ok = insert_async("test1"),
  <<"test1">> = read(),

  cql("DROP TABLE prepared_test;"),
  cql("CREATE TABLE IF NOT EXISTS prepared_test (a int PRIMARY KEY, b text);"),

  % The first query will be lost since the prepared statement has become invalid
  ok = insert_async("test2"),
  undefined = read(),

  ok = insert_async("test3"),
  <<"test3">> = read()
.

insert_async(Value) ->
  ecql:execute_async("INSERT INTO prepared_test (a, b) VALUES(?, ?)", [1, Value]),
  ecql:sync()
.

read() ->
  ecql:select_value("SELECT b FROM prepared_test WHERE a = ?", [1])
.

cql(Cql) ->
  ok = ecql:execute(Cql)
.
