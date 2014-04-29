%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_sender.erl - Queue outgoing messages
%%==============================================================================
-module(ecql_sender).
-behaviour(gen_server).

%% OTP gen_server
-export([
   init/1
  ,start_link/1
  ,stop/1
  ,handle_call/3
  ,handle_cast/2
  ,handle_info/2
  ,code_change/3
  ,terminate/2
]).

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% OTP gen_server API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
start_link(Socket) ->
  gen_server:start_link(?MODULE, Socket ,[])
.

%%------------------------------------------------------------------------------
init(Socket) ->
  {ok, Socket}
.

%%------------------------------------------------------------------------------
stop(Sender) ->
  gen_server:call(Sender, stop)
.

%%------------------------------------------------------------------------------
handle_call(stop, _From, State) ->
  {stop, normal, ok, State}
.

%%------------------------------------------------------------------------------
handle_cast(terminate ,State) ->
  {stop ,terminated ,State}
.

%%------------------------------------------------------------------------------
handle_info({send, Frame}, Socket) ->
   Frames = [Frame | receive_all_frames(100)]
  ,ok = gen_tcp:send(Socket, Frames)
  ,{noreply, Socket}
;
handle_info(timeout, State) ->
   % Who timed out?
   error_logger:error_msg("ecql_sender: Timeout occured~n")
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

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Private API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
% Collapsing all sends from the message queue
receive_all_frames(0) ->
  []
;
receive_all_frames(N) ->
  receive {send, Frame} ->
    [Frame | receive_all_frames(N - 1)]
  after 0 ->
    []
  end
.

%%==============================================================================
%% END OF FILE
