%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_async3.erl - Connector
%%==============================================================================
-module(ecql_async3).
-export([
  init/1
 ,test/1
]).

init(_Inserts) ->
   Table = "CREATE TABLE IF NOT EXISTS record_name (a int PRIMARY KEY, b int);"
  ,ecql:execute(Table)
.

test(Inserts) ->
   Nr = 10
  ,Main = self()
  ,lists:map(fun(Id) ->
    spawn(fun() ->
      Begin = trunc(((Id-1)*Inserts/Nr)+1)
     ,End = trunc(Begin - 1 + Inserts/Nr)
     ,lists:foreach(fun(Each) ->
        ecql:execute_async("INSERT INTO record_name (a, b) VALUES(?, ?)", [Each, Each])
      end, lists:seq(Begin, End))
      ,ecql:sync()
      ,Main ! done
      end)
  end, lists:seq(1, Nr))
  ,lists:foreach(fun(_) -> receive done -> ok end end, lists:seq(1, Nr))
  ,ok
.

%%==============================================================================
%% END OF FILE
