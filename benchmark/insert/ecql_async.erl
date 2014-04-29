%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_async.erl - Connector
%%==============================================================================
-module(ecql_async).
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
       ecql:execute_async("INSERT INTO record_name (a, b) VALUES(?, ?)", [Each, Each])
    end
   ,lists:seq(1, Inserts)
  )
.

%%==============================================================================
%% END OF FILE
