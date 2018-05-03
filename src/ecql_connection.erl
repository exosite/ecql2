%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_connection.erl - Connection keeper
%%==============================================================================
-module(ecql_connection).
-behaviour(gen_server).

%% Public API
-export([
   get_stream/1
  ,get_streams/1
]).

%% OTP gen_server
-export([
   init/1
  ,start/2
  ,start_link/2
  ,stop/1
  ,handle_call/3
  ,handle_cast/2
  ,handle_info/2
  ,code_change/3
  ,terminate/2
]).

%% Includes
-include("ecql.hrl").

%% Records
-record(state, {socket, pool, available, counter, sender, waiting = queue:new(), host}).
-record(preparedstatement, {cql, host, id, metadata, result_metadata}).

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
get_stream(Connection) ->
  gen_server:call(Connection, get_stream, infinity)
.

%%------------------------------------------------------------------------------
get_streams(Connection) ->
  tuple_to_list(gen_server:call(Connection, get_streams, infinity))
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% OTP gen_server API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
start(Host, Configuration) ->
   gen_server:start(?MODULE, {Host, Configuration}, [])
.

%%------------------------------------------------------------------------------
start_link(Host, Configuration) ->
   gen_server:start_link(?MODULE, {Host, Configuration}, [])
.

%%------------------------------------------------------------------------------
init({{Host, Port}, Configuration}) ->
   Buff = 1024*1024
  ,Opts = [{active, true}, {mode, binary}, {recbuf, Buff}, {sndbuf, Buff}]
  ,{ok, Socket} = gen_tcp:connect(Host, Port, Opts, 2000)
  ,ok = gen_tcp:send(Socket, frame_startup())
  ,User = proplists:get_value(user, Configuration, "cassandra")
  ,Pass = proplists:get_value(pass, Configuration, "cassandra")
  ,ok = auth(waitforframe(), User, Pass, Socket)
  ,PoolSize = proplists:get_value(streams_per_connection, Configuration, 100)
  ,{ok, Sender} = ecql_sender:start_link(Socket)
  ,Pool = init_pool(PoolSize, Sender, Host)
  ,{ok, #state{
     socket=Socket
    ,pool = list_to_tuple(Pool)
    ,available = Pool
    ,counter = 0
    ,sender = Sender
    ,host = {Host, Port}
  }}
.
init_pool(0, _Sender, _Host) ->
  []
;
init_pool(N, Sender, Host) ->
  init_pool(N-1, Sender, Host) ++ [init_stream(N, Sender, Host)]
.
init_stream(N, Sender, Host) ->
   {ok, Pid} = ecql_stream:start_link(self(), Sender, N-1, Host)
  ,Pid
.

%%------------------------------------------------------------------------------
stop(Connection) ->
  gen_server:cast(Connection, stop)
.

%%------------------------------------------------------------------------------
handle_call(get_stream, Client, State = #state{available = [], waiting = Waiting, counter = Counter}) ->
   {noreply, State#state{waiting = queue:in(Client, Waiting), counter = Counter + 1}}
;
handle_call(get_stream, {Pid, _Tag}, State = #state{available = Streams, counter = Counter, host = Host}) ->
   [Stream | NewAvailable] = Streams
  ,Stream ! {monitor, Pid}
  ,{reply, {Host, Stream}, State#state{available = NewAvailable, counter = Counter + 1}}
;
handle_call(get_streams, _From, State = #state{pool=Pool}) ->
  {reply, Pool, State}
.

%%------------------------------------------------------------------------------
handle_cast(stop, State = #state{pool = PoolTuple}) ->
   Pool = tuple_to_list(PoolTuple)
  ,[gen_server:cast(Stream, stop) || Stream <- Pool]
  ,erlang:send_after(10000, self(), stopnow)
  ,{noreply, State}
;
handle_cast(terminate ,State) ->
  {stop ,terminated ,State}
.

%%------------------------------------------------------------------------------
handle_info({add_stream, Stream}, State = #state{
   available = Streams
  ,waiting = Waiting
  ,host = Host
}) ->
  erlang:garbage_collect(Stream)
  ,case queue:out(Waiting) of
    {empty, Waiting}->
       {noreply, State#state{available = [Stream | Streams]}}
    ;
    {{value, Client = {Pid, _Tag}}, Waiting2} ->
       Stream ! {monitor, Pid}
      ,gen_server:reply(Client, {Host, Stream})
      ,{noreply, State#state{waiting = Waiting2}}
    %~
  end
;
handle_info(stopnow, State = #state{socket = Socket}) ->
   gen_tcp:close(Socket)
  ,{stop, normal, State}
;
handle_info({tcp_closed, _Socket}, State = #state{host = Host}) ->
   ets:match_delete(ecql_statements, #preparedstatement{host = Host, _ = '_'})
  ,{stop, tcp_closed, State}
;
handle_info({tcp, _Socket, Data}, State = #state{pool = Pool}) ->
   handle_data(Data, Pool)
  ,{noreply, State}
;
handle_info(timeout, State) ->
   % Who timed out?
   error_logger:error_msg("ecql_connection: Timeout occured~n")
  ,{noreply, State}
.

%%------------------------------------------------------------------------------
terminate(Reason, State) ->
  {Reason, State}
.

%%------------------------------------------------------------------------------
code_change(_ ,State ,_) ->
  {ok ,State}
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Private API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
handle_data(<<>>, _) ->
  ok
;
handle_data(Sofar, Pool) ->
   {#frame{opcode = OpCode, body = Body, stream = StreamId, flags = Flags}, Rest} = waitforframe(Sofar)
  ,element(StreamId + 1, Pool) ! {frame, OpCode, Body, Flags}
  ,handle_data(Rest, Pool)
.

%%------------------------------------------------------------------------------
auth({#frame{opcode=?OP_READY}, _}, _User, _Pass, _Socket) ->
  ok
;
auth({#frame{opcode=?OP_AUTHENTICATE}, _}, User, Pass, Socket) ->
   Body = [0, User, 0, Pass]
  ,ok = gen_tcp:send(Socket,
    frame(0, ?OP_AUTH_RESPONSE, [<<(iolist_size(Body)):?T_UINT32>>, Body])
  )
  ,case waitforframe() of
    {#frame{opcode=?OP_AUTH_SUCCESS}, _} ->
      ok
    ;
    {#frame{opcode=?OP_ERROR, body = <<Code:?T_INT32, Len:?T_UINT16, Message:Len/binary, _Rest/binary>>}, _} ->
      {error, Code, binary_to_list(Message)}
    %~
  end
;
auth(Other, _User, _Pass, _Socket) ->
  {error, {"Unknown Message", Other}}
.

%%------------------------------------------------------------------------------
waitforframe() ->
  waitforframe(<<>>)
.
% TODO ADD FLAGS
waitforframe(<<?VS_RESPONSE, Flags, StreamId:?T_INT16, OpCode, Length:?T_UINT32, FrameBody:Length/binary, Rest/binary>>) ->
  {#frame{stream=StreamId, opcode=OpCode, body=FrameBody, flags=Flags}, Rest}
;
waitforframe(<<?VS_RESPONSE, Flags, StreamId:?T_INT16, OpCode, Length:?T_UINT32, PartialFrameBody/binary>>) ->
   <<FrameBody:Length/binary, Rest/binary>> = waitforframe(Length, [PartialFrameBody])
  ,Flags =/= 0 andalso io:format("ohoh: ~p~n", [{#frame{stream=StreamId, opcode=OpCode, body=FrameBody, flags=Flags}}])
  ,{#frame{stream=StreamId, opcode=OpCode, body=FrameBody, flags=Flags}, Rest}
;
waitforframe(IncompleteHeader) ->
  receive {tcp, _Socket, Data} ->
    waitforframe(<<IncompleteHeader/binary, Data/binary>>)
  end
.
waitforframe(Length, PartialFrameBody) ->
  case (iolist_size(PartialFrameBody) < Length) of
    true ->
      receive {tcp, _Socket, Data} ->
        waitforframe(Length, [Data | PartialFrameBody])
      end
    ;
    false ->
      iolist_to_binary(lists:reverse(PartialFrameBody))
    %~
  end
.

%%------------------------------------------------------------------------------
frame(StreamId, OpCode, Body) ->
  [
     ?VS_REQUEST
    ,0
    ,<<StreamId:?T_UINT16>>
    ,OpCode
    ,<<(iolist_size(Body)):?T_UINT32>>
    ,Body
  ]
.
%%------------------------------------------------------------------------------
frame_startup() ->
  frame(0, ?OP_STARTUP, wire_map(["CQL_VERSION", "3.0.0"]))
.

%%------------------------------------------------------------------------------
wire_map(Strings) ->
   Length = round(length(Strings)/2)
  ,[<<Length:?T_UINT16>> | lists:map(fun wire_string/1, Strings)]
.

%%------------------------------------------------------------------------------
wire_string(String) ->
  [<<(iolist_size(String)):?T_UINT16>>, String]
.

%%==============================================================================
%% END OF FILE
