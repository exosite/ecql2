%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_batch_pure.erl - Connector
%%==============================================================================
-module(ecql_batch_pure).
-export([
  init/1
 ,test/1
]).

init(Inserts) ->
   Table = "CREATE TABLE IF NOT EXISTS record_name (a int PRIMARY KEY, b int);"
  ,ecql:execute(Table)
  ,put(?MODULE, [[Each, Each] || Each <- lists:seq(1, Inserts)])
.

test(_Inserts) ->
   Values = get(?MODULE)
  ,ecql:execute_batch("INSERT INTO record_name (a, b) VALUES(?, ?)", Values)
.

%%==============================================================================
%% END OF FILE
