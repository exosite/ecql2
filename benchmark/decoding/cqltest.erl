%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_stream.erl - Handler for one ongoing ecql stream
%%==============================================================================
-module(cqltest).

%% Public API
-export([
   test/2
]).

%%------------------------------------------------------------------------------
test(Mod, X) ->
  Data = test_data(),
  erlang:garbage_collect(),
  {Time, _} = timer:tc(fun do_test/3, [X, Mod, Data]),
  R = Time / X,
  io:format("~p ~p iterations: ~p~n", [Mod, X, R])
.
do_test(0, _, _) ->
  ok
;
do_test(X, Mod, P) ->
   lists:foreach(fun([A, B]) -> Mod:read_rows(A, B) end, P),
   do_test(X-1, Mod, P)
.

%%------------------------------------------------------------------------------
test_data() ->
  case get(data) of
    undefined ->
      {ok, Terms} = file:consult("data.io"),
      put(data, Terms),
      Terms
    ;
    Terms ->
      Terms
    %~
  end
.

%%==============================================================================
%% END OF FILE

