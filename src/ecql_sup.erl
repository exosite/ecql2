%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql.erl - Supervisor
%%==============================================================================
-module(ecql_sup).
-behaviour(supervisor).

-export([
   start_link/0
  ,init/1
]).

%%~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

%%------------------------------------------------------------------------------
start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%------------------------------------------------------------------------------
init(_) ->
   MaxRestart = 600
  ,MaxTime = 600
  ,{
     ok
    ,{
       {one_for_one, MaxRestart, MaxTime}
      ,[
        {
           ecql_etsman
          ,{ecql_etsman, start_link, []}
          ,permanent
          ,600
          ,worker
          ,[ecql_etsman]
        }
       ,{
           ecql_cache
          ,{ecql_cache, start_link, []}
          ,permanent
          ,600
          ,worker
          ,[ecql_cache]
        }
       ,{
           ecql
          ,{ecql, start_link, []}
          ,permanent
          ,600
          ,worker
          ,[ecql]
        }
       ,{
           ecql_log
          ,{ecql_log, start_link, []}
          ,permanent
          ,600
          ,worker
          ,[ecql_log]
        }
      ]
     }
   }
.

%%==============================================================================
%% END OF FILE
