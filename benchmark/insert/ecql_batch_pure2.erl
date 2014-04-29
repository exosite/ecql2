%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_batch_pure2.erl - Connector
%%==============================================================================
-module(ecql_batch_pure2).
-export([
  init/1
 ,test/1
]).

init(Inserts) ->
   Table = "CREATE TABLE IF NOT EXISTS record_name (a int PRIMARY KEY, b int);"
  ,ecql:execute(Table)
  ,put({?MODULE, 1}, [[Each, Each] || Each <- lists:seq(1, trunc(Inserts/2))])
  ,put({?MODULE, 2}, [[Each, Each] || Each <- lists:seq(trunc(Inserts/2), Inserts)])
.

test(_Inserts) ->
   do_test(1)
  ,do_test(2)
  ,receive done -> ok end
  ,receive done -> ok end
  ,ok
.

do_test(N) ->
   Pid = self()
  ,V = get({?MODULE, N})
  ,spawn(fun() ->
     ecql:execute_batch("INSERT INTO record_name (a, b) VALUES(?, ?)", V)
    ,Pid ! done
  end)
.

%%==============================================================================
%% END OF FILE
