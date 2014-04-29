%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_unlogged_batch.erl - Connector
%%==============================================================================
-module(ecql_unlogged_batch).
-export([
  init/1
 ,test/1
]).

init(_Inserts) ->
   Table = "CREATE TABLE IF NOT EXISTS record_name (a int PRIMARY KEY, b int);"
  ,ecql:execute(Table)
.

test(Inserts) ->
   Values = [[Each, Each] || Each <- lists:seq(1, Inserts)]
  ,ecql:execute_batch("INSERT INTO record_name (a, b) VALUES(?, ?)", Values, unlogged, default)
.

%%==============================================================================
%% END OF FILE
