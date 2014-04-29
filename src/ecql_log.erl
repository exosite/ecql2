%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_log.erl - Cache
%%==============================================================================
-module(ecql_log).
-behaviour(gen_server).

%% Public API
-export([
   log/4
  ,set_file/1
  ,set_slowthreshold/1
  ,set_logprobability/1
]).

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

%% Records
-record(state, {file = none, logprobability = 1, slowthreshold = 0, psum = 0}).


%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
set_file(Filename) ->
  gen_server:call(?MODULE, {set_file, Filename})
.

%%------------------------------------------------------------------------------
set_logprobability(Frac) ->
  gen_server:call(?MODULE, {set_logprobability, Frac})
.

%%------------------------------------------------------------------------------
set_slowthreshold(Microseconds) ->
  gen_server:call(?MODULE, {set_slowthreshold, Microseconds})
.

%%------------------------------------------------------------------------------
log(Time, Command, Cql, Args) ->
  gen_server:cast(?MODULE, {log, self(), Time, Command, Cql, Args})
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% OTP gen_server API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {} ,[])
.

%%------------------------------------------------------------------------------
init(_) ->
   {ok, LogFile} = application:get_env(log)
  ,case LogFile of
    Filename when is_list(Filename) ->
      {ok, Fp} = file:open(Filename, [binary, delayed_write, append])
    ;
    _ ->
      Fp = none
  end
  ,{ok, #state{file = Fp}}
.

%%------------------------------------------------------------------------------
stop() ->
  gen_server:call(?MODULE, stop)
.

%%------------------------------------------------------------------------------
handle_call({set_slowthreshold, Microseconds}, _From, State) ->
  {reply, ok, State#state{slowthreshold = Microseconds}}
;
handle_call({set_logprobability, Frac}, _From, State) ->
  {reply, ok, State#state{logprobability = Frac, psum = 0}}
;
handle_call({set_file, Filename}, _From, State = #state{file = Fp}) ->
   Fp =/= none andalso file:close(Fp)
  ,case is_list(Filename) andalso file:open(Filename, [binary, delayed_write, append]) of
     {ok, Fp2} -> {reply, ok, State#state{file = Fp2}}
    ;Other -> {reply, Other, State#state{file = none}}
  end
;
handle_call(stop, _From, State) ->
  {stop, normal, ok, State}
.

%%------------------------------------------------------------------------------
handle_cast({log, _Ref, _Time, _Command, _Cql, _Args} ,State = #state{file = none}) ->
  {noreply, State}
;
handle_cast({log, _Ref, Time, _Command, _Cql, _Args} ,State = #state{slowthreshold = Slow}) when Time < Slow->
  {noreply, State}
;
handle_cast({log, Ref, Time, Command, Cql, Args} ,State = #state{
  file = Fp, logprobability = Probality, psum = Sum
}) ->
  case ((Sum + Probality) >= 1) of
    true ->
      io:format(Fp, "~p: ~p ~p ~1024p ~1024p~n", [Ref, Time, Command, value(Cql), Args])
     ,{noreply, State#state{psum = Sum + Probality - 1}}
    ;
    false ->
      {noreply, State#state{psum = Sum + Probality}}
    %~
  end
;
handle_cast(terminate ,State) ->
  {stop ,terminated ,State}
.
value(Cql) when is_tuple(Cql) ->
  value(element(2, Cql))
;
value(Cql) when is_list(Cql) ->
  iolist_to_binary(Cql)
;
value(Cql) ->
  Cql
.

%%------------------------------------------------------------------------------
handle_info(timeout, State) ->
   % Who timed out?
   error_logger:error_msg("ecql_log: Timeout occured~n")
  ,{noreply, State}
.

%%------------------------------------------------------------------------------
terminate(_Reason, State) ->
  {shutdown, State}
.

%%------------------------------------------------------------------------------
code_change(_ ,State ,_) ->
  {ok ,State}
.

%%==============================================================================
%% END OF FILE
