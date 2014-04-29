%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_etsman.erl - Owns all ets tables to protectect them crashes.
%%==============================================================================
-module(ecql_etsman).
-behaviour(gen_server).

%% OTP gen_server
-export([
   init/1
  ,start_link/0
  ,stop/0
  ,handle_call/3
  ,handle_cast/2
  ,handle_info/2
  ,code_change/3
  ,terminate/2
]).

%% Includes
-include("ecql.hrl").

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% OTP gen_server API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {}, [])
.

%%------------------------------------------------------------------------------
init(_) ->
   ensure_ets_tables()
  ,{ok, {}}
.
ensure_ets_tables() ->
   ensure_ets_table(ecql_cache, [{write_concurrency, true}])
  ,lists:foreach(
    fun(Atom) ->
      ensure_ets_table(Atom, [{write_concurrency, true}])
    end
    ,?CACHE_SLICES_LIST
  )
  ,ensure_ets_table(ecql_statements, [{keypos, 2}])
.
ensure_ets_table(Name, Options) ->
  Self = self()
  ,case ets:info(Name, owner) of
    undefined ->
      Name = ets:new(
        Name, [named_table, public, {read_concurrency, true} | Options]
      )
    ;
    Self ->
      Name
    %~
  end
.

%%------------------------------------------------------------------------------
stop() ->
  gen_server:call(?MODULE, stop)
.

%%------------------------------------------------------------------------------
handle_call(stop, _From, State) ->
  {stop, normal, ok, State}
.

%%------------------------------------------------------------------------------
handle_cast(terminate, State) ->
  {stop, terminated, State}
.

%%------------------------------------------------------------------------------
handle_info(timeout, State) ->
   % Who timed out?
   error_logger:error_msg("ecql_etsman: Timeout occured~n")
  ,{noreply, State}
.

%%------------------------------------------------------------------------------
terminate(_Reason, State) ->
  {shutdown, State}
.

%%------------------------------------------------------------------------------
code_change(_, State, _) ->
   ensure_ets_tables()
  ,{ok, State}
.

%%==============================================================================
%% END OF FILE
