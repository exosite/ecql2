%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_stream.erl - Handler for one ongoing ecql stream
%%==============================================================================
-module(ecql_stream).
-behaviour(gen_server).
-compile(inline).
-compile({inline_size,   100}).
-on_load(init/0).


%% Public API
-export([
   foldl/6
  ,query/4
  ,query_async/4
  ,query_batch/5
  ,query_page/2
  ,query_page/4
  ,release/1
  ,sync/1
]).

%% OTP gen_server
-export([
   init/1
  ,start_link/4
  ,handle_call/3
  ,handle_cast/2
  ,handle_info/2
  ,code_change/3
  ,terminate/2
]).

%% Defines
-define(TIMEOUT, infinity).
-define(MAX_PENDING, 100).
-define(BATCH_SIZE, 2000).
-define(MAX_PENDING_BATCH, 5).
-define(DISABLED_PAGING, #paging{flag = 0}).

%% Includes
-include("ecql.hrl").

%% Records
-record(state, {
   connection, sender, stream, host, async_pending = 0, monitor_ref
  ,async_laststmt, async_start, laststmt, lastresult
}).
-record(metadata, {flags, columnspecs, paging_state, pk_index}).
-record(preparedstatement, {cql, host, id, metadata, result_metadata}).
-record(paging, {flag = 4, page_state = <<>>, page_size = 1000}).

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
foldl(Id, Fun, Acc, Cql, Args, Consistency) when is_atom(Consistency) ->
  foldl(Id, Fun, Acc, Cql, Args, consistency_to_int(Consistency))
;
foldl(Id, Fun, Acc, Cql, Args, Consistency) ->
  do_foldl(Id, Fun, Acc, Cql, Args, Consistency)
.
do_foldl({_, Pid} = Id, Fun, Acc, Cql, Args, Consistency) ->
   gen_server:cast(Pid, {query_start, <<>>, Cql, Args, Consistency})
  ,do_foldl_recv(Id, Fun, Acc, Cql, Args, Consistency)
.
do_foldl_recv({_, Pid} = Id, Fun, Acc, Cql, Args, Consistency) ->
  case gen_server:call(Pid, query_receive, ?TIMEOUT) of
    {<<>>, {Keys, Rows}} ->
       Fun(Keys, Rows, Acc)
    ;
    {PageState, {Keys, Rows}} when is_binary(PageState) ->
       gen_server:cast(Pid, {query_start, PageState, Cql, Args, Consistency})
      ,do_foldl_recv(Id, Fun, Fun(Keys, Rows, Acc), Cql, Args, Consistency)
    ;
    Other ->
      Other
    %~
  end
.

%%------------------------------------------------------------------------------
query(Id, Cql, Args, Consistency) when is_atom(Consistency) ->
  query(Id, Cql, Args, consistency_to_int(Consistency))
;
query(Id, Cql, Args, Consistency) ->
  case foldl(Id, fun do_query/3, {[], []}, Cql, Args, Consistency) of
    {Keys, [Rows]} ->
      {Keys, Rows}
    ;
    {Keys, Rows} when is_list(Keys) ->
      {Keys, lists:append(lists:reverse(Rows))}
    ;
    Other ->
      Other
    %~
  end
.
do_query(Keys, Rows, {_Keys, Rows0}) ->
  {Keys, [Rows | Rows0]}
.

%%------------------------------------------------------------------------------
query_async(Id, Cql, Args, Consistency) when is_atom(Consistency) ->
  query_async(Id, Cql, Args, consistency_to_int(Consistency))
;
query_async({_, Pid}, Cql, Args, Consistency) ->
  gen_server:call(Pid, {query_async, Cql, Args, Consistency, query, ?MAX_PENDING}, ?TIMEOUT)
.

%%------------------------------------------------------------------------------
query_batch(Id, Cql, ListOfArgs, Type, Consistency) when is_atom(Consistency) ->
  query_batch(Id, Cql, ListOfArgs, Type, consistency_to_int(Consistency))
;
query_batch(Id, Cql, ListOfArgs, logged, Consistency) ->
  query_batch(Id, Cql, ListOfArgs, 0, Consistency)
;
query_batch(Id, Cql, ListOfArgs, unlogged, Consistency) ->
  query_batch(Id, Cql, ListOfArgs, 1, Consistency)
;
query_batch(Id, Cql, ListOfArgs, counter, Consistency) ->
  query_batch(Id, Cql, ListOfArgs, 2, Consistency)
;
query_batch(Id, Cql, ListOfArgs, Type, Consistency) when is_integer(Type) ->
  case do_query_batch(Id, Cql, segment(ListOfArgs), Consistency, Type) of
    ok ->
      sync(Id)
    ;
    Error ->
      Error
    %~
  end
.
segment(ListOfArgs) ->
  {H, _, S} = lists:foldl(fun(Args, {Head, HeadSize, Segments}) ->
    Size = erts_debug:flat_size(Args) + (4 * length(Args)),
    case (HeadSize + Size) > ?BATCH_SIZE of
      true -> {[Args], Size, [Head | Segments]};
      false -> {[Args | Head], Size + HeadSize, Segments}
    end
  end, {[], 0, []}, ListOfArgs),
  [H | S]
.
do_query_batch(_, _, [], _, _) ->
  ok
;
do_query_batch(Id = {_, Pid}, Cql, [ListOfArgs | Tail], Consistency, Type) ->
  case gen_server:call(Pid, {query_async, Cql, ListOfArgs, Consistency, {batch, Type}, ?MAX_PENDING_BATCH}, ?TIMEOUT) of
    ok -> do_query_batch(Id, Cql, Tail, Consistency, Type);
    Error -> Error
  end
.


%%------------------------------------------------------------------------------
query_page(Id, Cql, Args, Consistency) when is_atom(Consistency) ->
  query_page(Id, Cql, Args, consistency_to_int(Consistency))
;
query_page(Id, Cql, Args, Consistency) ->
  do_query_page(Id, Cql, Args, Consistency, <<>>)
.
query_page(Id, {continuation, Cql, Args, Consistency, PageState}) ->
  do_query_page(Id, Cql, Args, Consistency, PageState)
;
query_page(_Id, '$end_of_table') ->
  '$end_of_table'
.
do_query_page({_, Pid}, Cql, Args, Consistency, OldPageState) ->
   gen_server:cast(Pid, {query_start, OldPageState, Cql, Args, Consistency})
  ,case gen_server:call(Pid, query_receive, ?TIMEOUT) of
    {<<>>, KeysRowsTuple} ->
       {KeysRowsTuple, '$end_of_table'}
    ;
    {PageState, KeysRowsTuple} when is_binary(PageState) ->
       {KeysRowsTuple, {continuation, Cql, Args, Consistency, PageState}}
    ;
    Other ->
      Other
    %~
  end
.

%%------------------------------------------------------------------------------
release({_, Pid}) ->
  gen_server:call(Pid, release, ?TIMEOUT)
.

%%------------------------------------------------------------------------------
sync({_, Pid}) ->
  gen_server:call(Pid, sync, ?TIMEOUT)
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Private loading
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
init() ->
  PrivDir = case code:priv_dir(?MODULE) of
    {error, bad_name} ->
       EbinDir = filename:dirname(code:which(?MODULE))
      ,AppPath = filename:dirname(EbinDir)
      ,filename:join(AppPath, "priv")
    ;
    Path ->
      Path
    %~
  end
  ,erlang:load_nif(filename:join(PrivDir, ecql_stream), 0)
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% OTP gen_server API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
start_link(Connection, Sender, StreamId, Host) ->
  gen_server:start_link(?MODULE, {Connection, Sender, StreamId, Host} ,[])
.

%%------------------------------------------------------------------------------
init({Connection, Sender, StreamId, Host}) ->
  {ok, #state{connection = Connection, sender = Sender, stream = StreamId, host = Host}}
.

%%------------------------------------------------------------------------------
handle_call(query_receive, _From, State = #state{lastresult = Ret}) ->
   {reply, Ret, State#state{lastresult = undefined}}
;
handle_call({query_async, Cql, ListOfArgs, Consistency, Type, Max}, _From, State) ->
   State1 = #state{async_pending = Pending} = wait_async(State, Max)
  ,{State2, Ret} = execute_query(Type, Cql, ListOfArgs, Consistency, State1, ?DISABLED_PAGING)
  ,case Ret of
    {ok, Statement} ->
      {reply, ok, State2#state{
        async_pending = Pending + num_queries(ListOfArgs),
        async_laststmt = Statement,
        async_start = erlang:timestamp()
      }}
    ;
    Err ->
      {reply, Err, State2}
    %~
  end
;
handle_call(release, _From, State = #state{monitor_ref = undefined}) ->
   {reply, {error, already_released}, State}
;
handle_call(release, _From, State = #state{connection = Conn, monitor_ref = MonitorRef}) ->
   demonitor(MonitorRef, [flush])
  ,Conn ! {add_stream, self()}
  ,{reply, ok, State#state{monitor_ref = undefined}}
;
handle_call(sync, _From, State) ->
  {reply, ok, wait_async(State)}
.

%%------------------------------------------------------------------------------
handle_cast({query_start, PageState, Statement, Args, Consistency} ,State0) ->
   State1 = wait_async(State0)
  ,{Time, {State2, Ret}} = timer:tc(fun() ->
    execute_query_block(
      Statement, Args, Consistency, State1, #paging{page_state = PageState}
    )
  end)
  ,ecql_log:log(Time, query, Statement, Args)
  ,{noreply, State2#state{laststmt = Statement, lastresult = Ret}}
;
handle_cast(stop, State) ->
   wait_async(State)
  ,{stop, normal, State}
.

%%------------------------------------------------------------------------------
% Async message incoming
handle_info({frame, ResponseOpCode0, ResponseBody0, Flags}, State = #state{async_pending = Pending}) ->
   {frame, ResponseOpCode, ResponseBody} = handle_flags(Flags, ResponseOpCode0, ResponseBody0)
  ,log(ResponseOpCode, ResponseBody, State)
  ,{noreply, State#state{async_pending = Pending - 1}}
;
handle_info({'DOWN', MonitorRef, _Type, _Object, _Info}, State = #state{connection = Conn, monitor_ref = MonitorRef}) ->
   Conn ! {add_stream, self()}
  ,{noreply, State#state{monitor_ref = undefined}}
;
handle_info({monitor, Client}, State = #state{monitor_ref = undefined}) ->
   Ref = monitor(process, Client)
  ,{noreply, State#state{monitor_ref = Ref}}
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
consistency_to_int(default) -> consistency_to_int(local_quorum);
consistency_to_int(one) -> 16#0001;
consistency_to_int(two) -> 16#0002;
consistency_to_int(three) -> 16#0003;
consistency_to_int(quorum) -> 16#0004;
consistency_to_int(all) -> 16#0005;
consistency_to_int(local_quorum) -> 16#0006;
consistency_to_int(each_quorum) -> 16#0007;
consistency_to_int(serial) -> 16#0008;
consistency_to_int(local_serial) -> 16#0009;
consistency_to_int(local_one) -> 16#000A.

%%------------------------------------------------------------------------------
wait_async(State) ->
  wait_async(State, 0)
.
wait_async(State = #state{async_pending = Pending}, Allowed) when Pending =< Allowed ->
  State
;
wait_async(State = #state{async_pending = Pending}, _Allowed) ->
   {frame, ResponseOpCode, ResponseBody} = receive_frame()
  ,log(ResponseOpCode, ResponseBody, State)
  ,wait_async(State#state{async_pending = Pending - 1})
.

%%------------------------------------------------------------------------------
log(ResponseOpCode, Body, State = #state{async_laststmt = Cql, async_start = Begin}) ->
  Time = case Begin of
    undefined -> -1;
    _ -> timer:now_diff(erlang:timestamp(), Begin)
  end
  ,ecql_log:log(Time, async, Cql, [])
  ,case ResponseOpCode of
    ?OP_ERROR ->
      do_log(handle_response(?OP_ERROR, Body), State)
    ;
    _ ->
      ok
    %~
  end
.
do_log({error, Code, Message}, State) ->
  error_logger:error_msg("query_async: failed: {error, ~p, ~s} state: ~p~n", [Code, Message, State])
;
do_log(Error, State) ->
  error_logger:error_msg("query_async: failed: ~p sate: ~p~n", [Error, State])
.

%%------------------------------------------------------------------------------
num_queries({multi, ListOfArgs}) ->
  length(ListOfArgs)
;
num_queries(_) ->
  1
.

%%------------------------------------------------------------------------------
execute_query(Type, Cql, Args, Consistency, State0, Paging) ->
  case prepare_statement(State0, Cql, Args) of
    {State1, {ok, Statement}} ->
      case catch compile_query(State1, Type, Statement, Args, Consistency, Paging) of
        {ok, Frame} ->
          ok = send(State1, Frame),
          {State1, {ok, Statement}}
        ;
        Err ->
          {State1, Err}
        %~
      end
    ;
    {State1, Error} ->
      {State1, Error}
    %~
  end
.

%%------------------------------------------------------------------------------
compile_query(State, Type, Cql, {multi, Args}, Consistency, Paging) ->
  {ok, [do_compile_query(State, Type, Cql, A, Consistency, Paging) || A <- Args]}
;
compile_query(State, Type, Cql, Args, Consistency, Paging) ->
  {ok, do_compile_query(State, Type, Cql, Args, Consistency, Paging)}
.
% Executes a single plain query
do_compile_query(State, query, Cql, [], Consistency, Paging) ->
  frame(State, ?OP_QUERY, [
     wire_longstring(Cql)
    ,<<
       Consistency:?T_UINT16
      ,(get_flag([], Paging, undefined)):?T_UINT8
     >>
    ,get_page_size(Paging)
    ,Paging#paging.page_state
  ])
;
% Executes a prepared query
do_compile_query(
   State
  ,query
  ,#preparedstatement{
    id = Id,
    metadata = RequestMetadata,
    result_metadata = #metadata{columnspecs = ResultColspec}
  }
  ,Args
  ,Consistency
  ,Paging
) ->
  frame(State, ?OP_EXECUTE, [
     <<
       (size(Id)):?T_UINT16
      ,Id/binary
      ,Consistency:?T_UINT16
      ,(get_flag(Args, Paging, ResultColspec)):?T_UINT8
     >>
    ,wire_values(Args, RequestMetadata)
    ,get_page_size(Paging)
    ,Paging#paging.page_state
  ])
;
% Executes a batch
do_compile_query(
   State
  ,{batch, Type}
  ,#preparedstatement{id = Id, metadata = #metadata{columnspecs = {_, RequestTypes}}}
  ,ListOfArgs
  ,Consistency
  ,_Paging
) ->
  frame(State, ?OP_BATCH, [
       Type
      ,<<(length(ListOfArgs)):?T_UINT16>>
    | wire_batch(Id, ListOfArgs, Consistency, RequestTypes)
  ])
.

%%------------------------------------------------------------------------------
execute_query_block(Cql, Args, Consistency, State0, Paging) ->
  case do_execute_query_block(Cql, Args, Consistency, State0, Paging) of
    {State1, {error, ?ER_UNPREPARED, _Msg}} ->
      % Retry
      do_execute_query_block(Cql, Args, Consistency, State1, Paging)
    ;
    Other ->
      Other
    %~
  end
.
do_execute_query_block(Cql, Args, Consistency, State0, Paging) ->
  case execute_query(query, Cql, Args, Consistency, State0, Paging) of
    {State1, {ok, Statement}} ->
      {State1, block_handle_response(Statement)}
    ;
    {State1, Err} ->
      {State1, Err}
    %~
  end
.

%%------------------------------------------------------------------------------
get_page_size(#paging{flag = Flag, page_size = PageSize}) ->
  if (Flag band 4) == 4 ->
    <<PageSize:32>>
  ;
  true ->
    <<>>
  end
.

%%------------------------------------------------------------------------------
%%  Function:
%%    get_flag(Args, Paging) -> flag
%%
%%  Description:
%%    Evaluate what value the flag would be
%%
%%    1: Values, 2: skip_metadata, 4: page_size, 8: with_paging_state,
%%    10: serial_consistency
%%------------------------------------------------------------------------------
get_flag([], #paging{flag = Flag, page_state = PGState}, _) ->
  get_paging_flag(Flag, PGState)
;
get_flag(_Args, #paging{flag = Flag, page_state = PGState}, undefined) ->
  1 + get_paging_flag(Flag, PGState)
;
get_flag(_Args, #paging{flag = Flag, page_state = PGState}, _) ->
  (3 + get_paging_flag(Flag, PGState))
.

%%------------------------------------------------------------------------------
%%  Function:
%%    get_paging_flag(Flag, PGState) -> paging flag number
%%
%%  Description:
%%    Evalute the paging flag number.
%%------------------------------------------------------------------------------
get_paging_flag(Flag, PGState) ->
   CheckFlag = Flag band 4
  ,case {CheckFlag, PGState} of
    {0, <<>>} ->
      0
    ;
    {0, _S} ->
      8
    ;
    {4, <<>>} ->
      4
    ;
    {4, _S} ->
      12
  end
.

%%------------------------------------------------------------------------------
prepare_statement(State0, Cql, []) ->
  {State0, {ok, Cql}}
;
prepare_statement(State0 = #state{host = Host}, Cql, _) ->
   Statement = iolist_to_binary(Cql)
  ,case ets:lookup(ecql_statements, {Host, Statement}) of
    [] ->
       State1 = wait_async(State0)
      ,send_frame(State1, ?OP_PREPARE, wire_longstring(Cql))
      ,Ret = case block_handle_response(Cql) of
        {ok, StatementRec0} ->
           StatementRec1 = StatementRec0#preparedstatement{cql = {Host, Statement}, host = Host}
          ,ets:insert(ecql_statements, StatementRec1)
          ,{ok, StatementRec1}
        ;
        Error ->
           Error
        %~
      end,
      {State1, Ret}
    ;
    [StatementRec = #preparedstatement{}] ->
       {State0, {ok, StatementRec}}
    %~
  end
.

%%------------------------------------------------------------------------------
send_frame(State, OpCode, Body) ->
  send(State, frame(State, OpCode, Body))
.

frame(#state{stream=StreamId}, OpCode, Body) ->
  [
     ?VS_REQUEST
    ,0
    ,<<StreamId:?T_INT16>>
    ,OpCode
    ,<<(iolist_size(Body)):?T_UINT32>>
    ,Body
  ]
.

send(#state{sender=Sender}, Frame) ->
  % 1) Sending to the 'Sender' allows batching gen_tcp:send's together
  % 2) Sending it directly to the gen_tcp:send's might distribute better
  % Based on testing 'Sender' approach performs 20% better on multithreaded loads
   Sender ! {send, Frame}
  ,ok
.

%%------------------------------------------------------------------------------
block_handle_response(Statement) ->
   {frame, ResponseOpCode, ResponseBody} = receive_frame()
  ,handle_response(ResponseOpCode, ResponseBody, Statement)
.

%%------------------------------------------------------------------------------
receive_frame() ->
  receive {frame, ResponseOpCode, ResponseBody, Flags} ->
    handle_flags(Flags, ResponseOpCode, ResponseBody)
  end
.

%%------------------------------------------------------------------------------
handle_flags(0, ResponseOpCode, ResponseBody) ->
  {frame, ResponseOpCode, ResponseBody}
;
% compression handle_flags(1, ResponseOpCode, ResponseBody)
handle_flags(8, ResponseOpCode, ResponseBody) ->
  {frame, ResponseOpCode, strip_warnings(ResponseBody)}
.

%%------------------------------------------------------------------------------
strip_warnings(<<Len:?T_UINT16, Rest/binary>>) ->
   {Errors, Rest1} = readn(Len, Rest, fun read_sbytes/1)
  ,error_logger:warning_msg("received warnings: ~p~n", [Errors])
  ,Rest1
.

%%------------------------------------------------------------------------------
handle_response(?OP_RESULT, <<?RT_ROWS, Body/binary>>, #preparedstatement{result_metadata = #metadata{columnspecs = ColSpecs}}) when ColSpecs =/= undefined ->
   {#metadata{paging_state = PageState}, Rest} = read_result_metadata(Body)
  ,{PageState, rows(Rest, ColSpecs)}
;
handle_response(?OP_RESULT, <<?RT_ROWS, Body/binary>>, _) ->
   {#metadata{columnspecs = ColSpecs, paging_state = PageState}, Rest} = read_result_metadata(Body)
  ,{PageState, rows(Rest, ColSpecs)}
;
handle_response(OpCode, Body, _) ->
  handle_response(OpCode, Body)
.

%%------------------------------------------------------------------------------
handle_response(?OP_ERROR, <<?ER_UNPREPARED:?T_INT32, Len:?T_UINT16, Message:Len/binary, IdLen:?T_UINT16, Id:IdLen/binary>>) ->
   ets:match_delete(ecql_statements, #preparedstatement{id = Id, _ = '_'})
  ,{error, ?ER_UNPREPARED, binary_to_list(Message)}
;
handle_response(?OP_ERROR, <<Code:?T_INT32, Len:?T_UINT16, Message:Len/binary, _Rest/binary>>) ->
  {error, Code, binary_to_list(Message)}
;
handle_response(?OP_RESULT, <<?RT_VOID, _/binary>>) ->
  ok
;
handle_response(?OP_RESULT, <<?RT_SETKEYSPACE, _/binary>>) ->
  ok
;
handle_response(?OP_RESULT, <<?RT_PREPARED, Len:?T_UINT16, Id:Len/binary, Body/binary>>) ->
   {Metadata, Rest} = read_metadata(Body)
  ,{ResultMetadata, <<>>} = read_result_metadata(Rest)
  ,{ok, #preparedstatement{id = Id, metadata = Metadata, result_metadata = ResultMetadata}}
;
handle_response(?OP_RESULT, <<?RT_SCHEMACHANGE, _/binary>>) ->
  ok
;
handle_response(OpCode, Binary) ->
  {error, -1, {notyetimplemented, OpCode, Binary}}
.

%%------------------------------------------------------------------------------
readn(0, Body, _Fun) ->
  {[], Body}
;
readn(1, Body, Fun) ->
  {Value, Rest} = Fun(Body),
  {[Value], Rest}
;
readn(2, Body, Fun) ->
   {V1, R1} = Fun(Body)
  ,{V2, R2} = Fun(R1)
  ,{[V1, V2], R2}
;
readn(3, Body, Fun) ->
   {V1, R1} = Fun(Body)
  ,{V2, R2} = Fun(R1)
  ,{V3, R3} = Fun(R2)
  ,{[V1, V2, V3], R3}
;
readn(4, Body, Fun) ->
   {V1, R1} = Fun(Body)
  ,{V2, R2} = Fun(R1)
  ,{V3, R3} = Fun(R2)
  ,{V4, R4} = Fun(R3)
  ,{[V1, V2, V3, V4], R4}
;
readn(N, Body, Fun) ->
   {V1, R1} = Fun(Body)
  ,{V2, R2} = Fun(R1)
  ,{V3, R3} = Fun(R2)
  ,{V4, R4} = Fun(R3)
  ,{VList, R5} = readn(N-4, R4, Fun)
  ,{[V1, V2, V3, V4 | VList], R5}
.

%%------------------------------------------------------------------------------
% <global_table_spec> is present if the Global_tables_spec is set in
% <flags>. If present, it is composed of two [string] representing the
% (unique) keyspace name and table name the columns return are of.
read_tablespec(Body) ->
   <<
     KLen:?T_UINT16, KeySpace:KLen/binary
    ,TLen:?T_UINT16, TableName:TLen/binary
    ,Rest/binary
   >> = Body
  ,{{KeySpace, TableName}, Rest}
.

%%------------------------------------------------------------------------------
rows(Body, {Keys, Types}) ->
  try read_rows(Body, Types) of
    {Rows, <<>>} ->
      {Keys, Rows}
  %~
  catch
    error:badarg ->
       {Rows, <<>>} = read_rows_erl(Body, Types)
      ,{Keys, Rows}
  end
.

%%------------------------------------------------------------------------------
read_rows(Body, Types) ->
  read_rows_erl(Body, Types)
.

read_rows_erl(<<RowCount:?T_INT32, Body/binary>>, ColTypes) ->
   TLen = length(ColTypes)
  ,{Cells, Rest} = read_n_bytes(RowCount*TLen, Body)
  ,{convert_rows(ColTypes, RowCount, Cells), Rest}
.

convert_rows(_ColTypes, 0, []) ->
  []
;
convert_rows(ColTypes, 1, Cells) ->
   {Row, []} = convert_row(ColTypes, Cells)
  ,[Row]
;
convert_rows(ColTypes, 2, Cells) ->
   {Row, Rest} = convert_row(ColTypes, Cells), {Row2, []} = convert_row(ColTypes, Rest)
  ,[Row, Row2]
;
convert_rows(ColTypes, 3, Cells) ->
   {Row, Rest} = convert_row(ColTypes, Cells), {Row2, Rest2} = convert_row(ColTypes, Rest)
  ,{Row3, []} = convert_row(ColTypes, Rest2)
  ,[Row, Row2, Row3]
;
convert_rows(ColTypes, 4, Cells) ->
   {Row, Rest} = convert_row(ColTypes, Cells), {Row2, Rest2} = convert_row(ColTypes, Rest)
  ,{Row3, Rest3} = convert_row(ColTypes, Rest2), {Row4, []} = convert_row(ColTypes, Rest3)
  ,[Row, Row2, Row3, Row4]
;
convert_rows(ColTypes, N, Cells) ->
   {Row, Rest} = convert_row(ColTypes, Cells), {Row2, Rest2} = convert_row(ColTypes, Rest)
  ,{Row3, Rest3} = convert_row(ColTypes, Rest2), {Row4, Rest4} = convert_row(ColTypes, Rest3)
  ,Rows = convert_rows(ColTypes, N-4, Rest4)
  ,[Row, Row2, Row3, Row4 | Rows]
.

convert_row([], C) ->
  {[], C}
;
convert_row([T1], [C1 | C]) ->
  {[convert(T1, C1)], C}
;
convert_row([T1, T2], [C1, C2 | C]) ->
  {[convert(T1, C1), convert(T2, C2)], C}
;
convert_row([T1, T2, T3], [C1, C2, C3 | C]) ->
  {[convert(T1, C1), convert(T2, C2), convert(T3, C3)], C}
;
convert_row([T1, T2, T3, T4], [C1, C2, C3, C4 | C]) ->
  {[convert(T1, C1), convert(T2, C2), convert(T3, C3), convert(T4, C4)], C}
;
convert_row([T1, T2, T3, T4 | T], [C1, C2, C3, C4 | C]) ->
   {VList, Rest} = convert_row(T, C)
  ,{[convert(T1, C1), convert(T2, C2), convert(T3, C3), convert(T4, C4) | VList], Rest}
.

%%------------------------------------------------------------------------------
read_n_bytes(0, Body) ->
  {[], Body}
;
read_n_bytes(1, Body) ->
   {V, R} = read_bytes(Body)
  ,{[V], R}
;
read_n_bytes(2, Body) ->
   {V, R} = read_bytes(Body), {V2, R2} = read_bytes(R)
  ,{[V, V2], R2}
;
read_n_bytes(3, Body) ->
   {V, R} = read_bytes(Body), {V2, R2} = read_bytes(R)
  ,{V3, R3} = read_bytes(R2)
  ,{[V, V2, V3], R3}
;
read_n_bytes(4, Body) ->
   {V, R} = read_bytes(Body), {V2, R2} = read_bytes(R)
  ,{V3, R3} = read_bytes(R2), {V4, R4} = read_bytes(R3)
  ,{[V, V2, V3, V4], R4}
;
read_n_bytes(N, Body) ->
   {V, R} = read_bytes(Body), {V2, R2} = read_bytes(R)
  ,{V3, R3} = read_bytes(R2), {V4, R4} = read_bytes(R3)
  ,{VList, R5} = read_n_bytes(N-4, R4)
  ,{[V, V2, V3, V4 | VList], R5}
.

%%------------------------------------------------------------------------------
% [bytes]: A [int] n, followed by n bytes if n >= 0. If n < 0,
%          no byte should follow and the value represented is `null`.
read_bytes(<<Len:?T_INT32, Value:Len/binary, Rest/binary>>) ->
  {Value, Rest}
;
read_bytes(<<Len:?T_INT32, Rest/binary>>) when Len < 0 ->
  {undefined, Rest}
.

%%------------------------------------------------------------------------------
% [short bytes]  A [short] n, followed by n bytes if n >= 0.
read_sbytes(<<Len:?T_UINT16, Value:Len/binary, Rest/binary>>) ->
  {Value, Rest}
.

%%------------------------------------------------------------------------------
% The <column_name> is a [string] and <type> is an [option]
read_colspec(<<Len:?T_INT16, Name:Len/binary, Type:?T_INT16, Rest/binary>>) ->
   {TypeDef, Rest2} = read_colspec_type(Type, Rest)
  ,{{binary_to_atom(Name, utf8), TypeDef}, Rest2}
.


%%------------------------------------------------------------------------------
read_colspec_type(<<Type:?T_INT16, Rest/binary>>) ->
  read_colspec_type(Type, Rest)
.

%%------------------------------------------------------------------------------
read_colspec_type(0, _) ->
  undefined
;
read_colspec_type(16#20, Rest) ->
   {ValueType, Rest2} = read_colspec_type(Rest)
  ,{{list, ValueType}, Rest2}
;
read_colspec_type(16#21, Rest) ->
   {[KeyType, ValueType], Rest2} = readn(2, Rest, fun read_colspec_type/1)
  ,{{map, {KeyType, ValueType}}, Rest2}
;
read_colspec_type(16#22, Rest) ->
   {ValueType, Rest2} = read_colspec_type(Rest)
  ,{{set, ValueType}, Rest2}
;
read_colspec_type(16#30, <<KLen:?T_INT16, Ks:KLen/binary, NLen:?T_INT16, Name:NLen/binary, N:?T_INT16, Rest/binary>>) ->
  {ValueTypeMap, Rest1} = readn(N, Rest, fun(<<FLen:?T_INT16, FieldName:FLen/binary, R/binary>>) ->
    {Type, R1} = read_colspec_type(R),
    {{list_to_atom(binary_to_list(FieldName)), Type}, R1}
  end)
  ,{Names, Types} = lists:unzip(ValueTypeMap)
  ,{{udt, Ks, Name, Names, Types}, Rest1}
;
read_colspec_type(16#31, <<Len:?T_INT16, Rest/binary>>) ->
   {ValueTypes, Rest2} = readn(Len, Rest, fun read_colspec_type/1)
  ,{{tuple, ValueTypes}, Rest2}
;
read_colspec_type(Type, Rest) ->
  {Type, Rest}
.


%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% START OF PROTOCOL SPLIT
%% Native Protocol V4 Only
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
-ifndef(native_protocol_3).

%%------------------------------------------------------------------------------
read_short(<<Value:?T_UINT16, Rest/binary>>) ->
  {Value, Rest}
.

%%------------------------------------------------------------------------------
read_primary_keys(<<PkCount:?T_INT32, Body/binary>>) ->
  % Returns {Pks, Rest}
  readn(PkCount, Body, fun read_short/1)
.

%%------------------------------------------------------------------------------
read_metadata(<<Flags:?T_INT32, ColCount:?T_INT32, Body0/binary>>) ->
   {Pks, Body1} = read_primary_keys(Body0)
  ,{Meta, Body2} = read_result_metadata(Flags, <<>>, ColCount, Body1)
  ,{Meta#metadata{pk_index = Pks}, Body2}
.

-else.
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Native Protocol V3 Only
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
read_metadata(Bin) ->
   read_result_metadata(Bin)
.

-endif.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% END OF PROTOCOL SPLIT
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
read_result_metadata(<<Flag0:?T_INT32, ColCount:?T_INT32, Body0/binary>>) ->
   {Flag1, PageState, Body1} = retrieve_pagestate(Flag0, Body0)
  ,read_result_metadata(Flag1, PageState, ColCount, Body1)
.
% table spec per column
read_result_metadata(0, PageState, ColCount, Body) ->
   {ColSpecs, Rest} = readn(ColCount, Body, fun(ColSpecBin) ->
     {_TableSpec, ColSpecBinRest0} = read_tablespec(ColSpecBin)
    ,read_colspec(ColSpecBinRest0)
   end)
  ,{#metadata{flags = 0, columnspecs = format_specs(ColSpecs)
    ,paging_state = PageState}, Rest}
;
% global table spec only once
read_result_metadata(1, PageState, ColCount, Body) ->
   {_TableSpec, Rest0} = read_tablespec(Body)
  ,{ColSpecs, Rest1} = readn(ColCount, Rest0, fun read_colspec/1)
  ,{#metadata{flags = 1, columnspecs = format_specs(ColSpecs)
    ,paging_state = PageState}, Rest1}
;
% no metadata
read_result_metadata(4, PageState, _ColCount, Body) ->
  {#metadata{flags = 4, paging_state = PageState}, Body}
;
% no metadata + global table spec is actually the same
read_result_metadata(5, PageState, _ColCount, Body) ->
  {#metadata{flags = 5, paging_state = PageState}, Body}
.

%%------------------------------------------------------------------------------
%%  Function:
%%    retrieve_pagestate(Flag, Body) -> {NewFlag, PageState, NewBody}
%%
%%  Description:
%%    Check if the flag of Has_more_page is set.  If it is set, PageState will
%%    include the value of paging_state else PageState is <<>>.
%%    If Has_more_page is set, Flag and Body will also be updated (remove
%%    Has_more_page from Flag and remove paging_state from Body.)
%%------------------------------------------------------------------------------
retrieve_pagestate(Flags, Body0) when (2 band Flags) == 2 ->
   <<N:?T_INT32, PageState:N/binary, Body1/binary>> = Body0
  ,{Flags bxor 2, <<N:?T_INT32, PageState:N/binary>>, Body1}
;
retrieve_pagestate(Flags, Body) ->
  {Flags, <<>>, Body}
.

%%------------------------------------------------------------------------------
format_specs(ColSpecs) ->
  lists:unzip(ColSpecs)
.

%%------------------------------------------------------------------------------
% CQL null == undefined
convert(_, undefined) ->
  undefined
;
% 0x0001    Ascii
convert(1, Value) ->
  % Change for Elixir string as binary semantic
  % binary_to_list(Value)
  Value
;
% 0x0002    Bigint
convert(2, Value) ->
  convert_int(Value)
;
% 0x0003    Blob
convert(3, Value) ->
  Value
;
% 0x0004    Boolean
convert(4, <<0>>) ->
  false
;
convert(4, <<1>>) ->
  true
;
% 0x0005    Counter
convert(5, Value) ->
  convert_int(Value)
;
% 0x0006    Decimal
convert(6, <<Scale:?T_INT32, Unscaled/binary>>) ->
  {Scale, convert_int(Unscaled)}
;
% NOPE
% 0x0007    Double
convert(7, Value) ->
  convert_float(Value)
;
% 0x0008    Float
convert(8, Value) ->
  convert_float(Value)
;
% 0x0009    Int
convert(9, Value) ->
  convert_int(Value)
;
% 0x000B    Timestamp
convert(16#B, Value) ->
  convert_int(Value)
;
% 0x000C    Uuid
convert(16#C, Value) ->
  convert_uuid(Value)
;
% 0x000D    Varchar
convert(16#D, Value) ->
  % Change for Elixir string as binary semantic
  % binary_to_list(Value)
  Value
;
% 0x000E    Varint
convert(16#E, Value) ->
  convert_int(Value)
;
% 0x000F    Timeuuid
convert(16#F, Value) ->
  convert_uuid(Value)
;
% 0x0010    Inet
convert(16#10, <<A:?T_UINT8,B:?T_UINT8,C:?T_UINT8,D:?T_UINT8>>) ->
  {A, B, C, D}
;
convert(
    16#10,
    <<
      A:?T_UINT16,B:?T_UINT16,C:?T_UINT16,D:?T_UINT16,
      E:?T_UINT16,F:?T_UINT16,G:?T_UINT16,H:?T_UINT16
    >>) ->
  {A, B, C, D, E, F, G, H}
;
% NOPE
% 0x0011    Date
convert(16#11, Value) ->
  calendar:gregorian_days_to_date(date_base() + convert_int(Value))
;
% 0x0012    Time
convert(16#12, Value) ->
  convert_int(Value)
;
% 0x0013    Smallint
convert(16#13, Value) ->
  convert_int(Value)
;
% 0x0014    Tinyint
convert(16#14, Value) ->
  convert_int(Value)
;
% 0x0020    List: the value is an [option], representing the type
%                of the elements of the list.
convert({list, ValueType}, <<Count:?T_INT32, Body/binary>>) ->
   {Values, <<>>} = readn(Count, Body, fun read_bytes/1)
  ,lists:map(fun(Value) -> convert(ValueType, Value) end, Values)
;
% 0x0021    Map: the value is two [option], representing the types of the
%               keys and values of the map
convert({map, {KeyType, ValueType}}, <<Count:?T_INT32, Body/binary>>) ->
  {Values, <<>>} = readn(Count, Body, fun(BinRow) ->
     {[Key, Value], RowRest} = readn(2, BinRow, fun read_bytes/1)
    ,{{convert(KeyType, Key), convert(ValueType, Value)}, RowRest}
  end)
  ,maps:from_list(Values)
;
% 0x0022    Set: the value is an [option], representing the type
%                of the elements of the set
convert({set, ValueType}, Binary) ->
  convert({list, ValueType}, Binary)
;
% 0x0030    UDT
convert({udt, _Ks, _Name, Names, Types}, Binary) ->
  {Values, <<>>} = read_n_bytes(length(Types), Binary),
  {Result, []} = convert_row(Types, Values),
  maps:from_list(lists:zip(Names, Result))
;
% 0x0031    Tuple
convert({tuple, Types}, Binary) ->
  {Values, <<>>} = read_n_bytes(length(Types), Binary),
  {Result, []} = convert_row(Types, Values),
  list_to_tuple(Result)
.

%%------------------------------------------------------------------------------
convert_float(<<Value:?T_DOUBL>>) ->
   Value
;
convert_float(<<Value:?T_FLOAT>>) ->
   Value
.

%%------------------------------------------------------------------------------
convert_int(<<Value:?T_INT64>>) ->
   Value
;
convert_int(<<Value:?T_INT32>>) ->
   Value
;
convert_int(<<Value:?T_INT16>>) ->
   Value
;
convert_int(<<Value:?T_INT8>>) ->
   Value
.

%%------------------------------------------------------------------------------
convert_uuid(<<A:32, B:16, C:16, D:16, E:48>>) ->
  iolist_to_binary(io_lib:format(
    "~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
    [A, B, C, D, E]
  ))
.

%%------------------------------------------------------------------------------
wire_values(Values, RequestMetadata) ->
   #metadata{columnspecs = {_, RequestTypes}} = RequestMetadata
  ,wire_shortlist(zipwith_wire(RequestTypes, Values))
.

%%------------------------------------------------------------------------------
zipwith_wire([T1], [V1]) ->
  [wire_longstring(wire_value(T1, V1))]
;
zipwith_wire([T1, T2], [V1, V2]) ->
  [wire_longstring(wire_value(T1, V1)), wire_longstring(wire_value(T2, V2))]
;
zipwith_wire([T1, T2, T3], [V1, V2, V3]) ->
  [
    wire_longstring(wire_value(T1, V1)), wire_longstring(wire_value(T2, V2)),
    wire_longstring(wire_value(T3, V3))
  ]
;
zipwith_wire([T1, T2, T3, T4], [V1, V2, V3, V4]) ->
  [
    wire_longstring(wire_value(T1, V1)), wire_longstring(wire_value(T2, V2)),
    wire_longstring(wire_value(T3, V3)), wire_longstring(wire_value(T4, V4))
  ]
;
zipwith_wire([T1, T2, T3, T4 | T], [V1, V2, V3, V4 | V]) ->
  [
    wire_longstring(wire_value(T1, V1)), wire_longstring(wire_value(T2, V2)),
    wire_longstring(wire_value(T3, V3)), wire_longstring(wire_value(T4, V4))
    |zipwith_wire(T, V)
  ]
.

%%------------------------------------------------------------------------------
wire_batch(Id, ListOfArgs, Consistency, RequestTypes) ->
  Head = <<
     1:?T_UINT8 % kind == 'prepared query'
    ,(size(Id)):?T_UINT16
    ,Id/binary
    ,(length(RequestTypes)):?T_UINT16
  >>
  ,do_wire_batch(Head, ListOfArgs, Consistency, RequestTypes)
.
do_wire_batch(Head, [Args | ListOfArgs], Consistency, RequestTypes) ->
  [
     Head
    ,zipwith_wire(RequestTypes, Args)
    |do_wire_batch(Head, ListOfArgs, Consistency, RequestTypes)
  ]
;
do_wire_batch(_Head, [], Consistency, _RequestTypes) ->
  [
     <<Consistency:?T_UINT16>>
    ,0
  ]
.

%%------------------------------------------------------------------------------
wire_value(Type, Value) ->
  try
    do_wire_value(Type, Value)
  catch
    _:Reason ->
      TypeString = case Type of
        I when is_integer(I) ->
          ["0x" | integer_to_list(Type, 16)]
        ;
        {A, _} when is_atom(A) ->
          atom_to_list(A)
        %~
      end,
      throw({error, encoding, iolist_to_binary(io_lib:format(
        "Value type ~s received invalid value ~p~n~p",
        [TypeString, Value, Reason]
      ))})
    %~
  end
.

do_wire_value(_, undefined) ->
  undefined
;
% 0x0001    Ascii
do_wire_value(1, Value) when is_list(Value) or is_binary(Value) ->
  Value
;
do_wire_value(1, Value) when is_atom(Value) ->
  atom_to_binary(Value, utf8)
;
% 0x0002    Bigint
do_wire_value(2, Value) ->
  wire_bigint(Value)
;
% 0x0003    Blob
do_wire_value(3, Value) when is_binary(Value) or is_list(Value) ->
  Value
;
% 0x0004    Boolean
do_wire_value(4, 0) ->
  <<0>>
;
do_wire_value(4, false) ->
  <<0>>
;
do_wire_value(4, true) ->
  <<1>>
;
do_wire_value(4, _) ->
  <<1>>
;
% 0x0005    Counter
do_wire_value(5, Value) ->
  wire_bigint(Value)
;
% 0x0006    Decimal
do_wire_value(6, {Scale, Unscaled}) ->
  [wire_int(Scale), wire_bigint(Unscaled)]
;
do_wire_value(6, Value) when is_integer(Value) ->
  do_wire_value(6, {0, Value})
;
do_wire_value(6, Value) when is_float(Value) ->
  do_wire_value(6, {3, Value*1000})
;
% 0x0007    Double
do_wire_value(7, Value) ->
  <<Value:?T_DOUBL>>
;
% 0x0008    Float
do_wire_value(8, Value) ->
  <<Value:?T_FLOAT>>
;
% 0x0009    Int
do_wire_value(9, Value) ->
  wire_int(Value)
;
% 0x000B    Timestamp
do_wire_value(16#B, Value) ->
  wire_bigint(Value)
;
% 0x000C    Uuid
do_wire_value(16#C, Value) ->
  wire_uuid(Value)
;
% 0x000D    Varchar
do_wire_value(16#D, Value) when is_binary(Value) or is_list(Value) ->
  Value
;
do_wire_value(16#D, Value) when is_atom(Value) ->
  atom_to_binary(Value, utf8)
;
% 0x000E    Varint
do_wire_value(16#E, Value) ->
  wire_bigint(Value)
;
% 0x000F    Timeuuid
do_wire_value(16#F, Value) ->
  wire_uuid(Value)
;
% 0x0010    Inet
do_wire_value(16#10, {A, B, C, D}) ->
  <<A:?T_UINT8,B:?T_UINT8,C:?T_UINT8,D:?T_UINT8>>
;
do_wire_value(16#10, {A, B, C, D, E, F, G, H}) ->
  <<
    A:?T_UINT16,B:?T_UINT16,C:?T_UINT16,D:?T_UINT16,
    E:?T_UINT16,F:?T_UINT16,G:?T_UINT16,H:?T_UINT16
  >>
;
do_wire_value(16#10, Value) when is_binary(Value) ->
  do_wire_value(16#10, binary_to_list(Value))
;
do_wire_value(16#10, Value) when is_list(Value) ->
  {ok, Address} = inet:parse_address(Value),
  do_wire_value(16#10, Address)
;
% 0x0011    Date
do_wire_value(16#11, Value = {_Year, _Month, _Date}) ->
  Date = calendar:date_to_gregorian_days(Value),
  wire_int(Date - date_base())
;
% 0x0012    Time
do_wire_value(16#12, Value) ->
  wire_bigint(Value)
;
% 0x0013    Smallint
do_wire_value(16#13, Value) ->
  <<Value:?T_INT16>>
;
% 0x0014    Tinyint
do_wire_value(16#14, Value) ->
  <<Value:?T_INT8>>
;
% 0x0020    List: the value is an [option], representing the type
%                of the elements of the list.
do_wire_value({list, ValueType}, Values) when is_list(Values) ->
  [<<(length(Values)):?T_INT32>> | lists:map(fun(Value) ->
    wire_longstring(do_wire_value(ValueType, Value))
  end, Values)]
;
do_wire_value({list, ValueType}, Value) ->
  do_wire_value({list, ValueType}, [Value])
;
% 0x0021    Map: the value is two [option], representing the types of the
%               keys and values of the map
do_wire_value(Type = {map, _}, Values) when is_list(Values) ->
  do_wire_value(Type, maps:from_list(Values))
;
do_wire_value({map, {KeyType, ValueType}}, Values) when is_map(Values) ->
  [<<(maps:size(Values)):?T_INT32>> | maps:fold(fun(Key, Value, Acc) -> [
     wire_longstring(do_wire_value(KeyType, Key))
    ,wire_longstring(do_wire_value(ValueType, Value))
  | Acc] end, [], Values)]
;
do_wire_value({map, ValueType}, Value) when is_tuple(Value) ->
  do_wire_value({map, ValueType}, [Value])
;
% 0x0022    Set: the value is an [option], representing the type
%                of the elements of the set
do_wire_value({set, ValueType}, Value) ->
  do_wire_value({list, ValueType}, Value)
;
% 0x0030    UDT
do_wire_value({udt, _Ks, _Name, Names, Types}, Map) ->
  Values = [maps:get(Key, Map, undefined) || Key <- Names],
  zipwith_wire(Types, Values)
;
% 0x0031    Tuple
do_wire_value({tuple, Types}, Values) ->
  zipwith_wire(Types, tuple_to_list(Values))
.

%%------------------------------------------------------------------------------
wire_bigint(Value) when is_float(Value) ->
  wire_bigint(trunc(Value))
;
wire_bigint(Value) ->
  <<Value:?T_INT64>>
.

%%------------------------------------------------------------------------------
wire_int(Value) when Value > 2147483647 ->
   error_logger:error_msg("wire_int(): truncating integer ~p~n", [Value])
  ,wire_int(2147483647)
;
wire_int(Value) ->
  <<Value:?T_INT32>>
.

%%------------------------------------------------------------------------------
wire_longstring(undefined) ->
  <<-1:?T_INT32>>
;
wire_longstring(Value) when is_binary(Value) ->
  <<(size(Value)):?T_INT32, Value/binary>>
;
wire_longstring(Value) when is_list(Value) ->
  [<<(iolist_size(Value)):?T_INT32>>, Value]
%~ ;
%~ wire_longstring(Value) when is_atom(Value) ->
  %~ wire_longstring(atom_to_binary(Value, utf8))
.

%%------------------------------------------------------------------------------
wire_shortlist(Value) ->
  [<<(length(Value)):?T_UINT16>>, Value]
.

%%------------------------------------------------------------------------------
wire_uuid(Value) when byte_size(Value) == 16 ->
  Value
;
wire_uuid(Value) when byte_size(Value) == 32 ->
  {ok, [Int], []} = io_lib:fread("~16u", binary_to_list(Value)),
  <<Int:128>>
;
wire_uuid(Value) when byte_size(Value) == 36 ->
  {ok, [A, B, C, D, E], []} = io_lib:fread("~16u-~16u-~16u-~16u-~16u", binary_to_list(Value)),
  <<A:32, B:16, C:16, D:16, E:48>>
;
wire_uuid(Value) when is_list(Value) ->
  wire_uuid(list_to_binary(Value))
;
wire_uuid(Value) when is_integer(Value) ->
  wire_uuid(<<Value:128>>)
.

%%------------------------------------------------------------------------------
date_base() ->
  calendar:date_to_gregorian_days({1970, 1, 1})
.

%%==============================================================================
%% END OF FILE
