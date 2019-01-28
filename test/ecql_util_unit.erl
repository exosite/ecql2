-module(ecql_util_unit).
-export([tests/0]).
-include_lib("eunit/include/eunit.hrl").

% Testing batch features

tests() ->
  {setup, fun start/0, fun stop/1, [
    {"host", {timeout, 60, fun host/0}}
  ]}
.

start() ->
  {ok, _Apps} = application:ensure_all_started(ecql)
.

stop(_) ->
  ok
.

%% TESTS START HERE
host() ->
  io:format("connected to host ~p ~n", [ecql:host()]),
  Ip = ip(),
  Ip = ecql:host()
.

ip() ->
  {ok, [{Ip, _Port}]} = application:get_env(ecql, hosts),
  Ip
.