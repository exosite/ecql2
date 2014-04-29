%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_async.erl - Connector
%%==============================================================================
-module(ecql_async2).
-export([
  init/1
 ,test/1
]).

init(_Inserts) ->
   Table = "CREATE TABLE IF NOT EXISTS record_name (a int PRIMARY KEY, b int);"
  ,ecql:execute(Table)
.

test(Inserts) ->
  lists:foreach(
    fun(Each) ->
       I = Each*2,
       ecql:execute_async("INSERT INTO record_name (a, b) VALUES(?, ?)", {multi, [[I-1, I-1], [I, I]]})
    end
   ,lists:seq(1, trunc(Inserts/2))
  ),
  ecql:sync()
.

%%==============================================================================
%% END OF FILE
