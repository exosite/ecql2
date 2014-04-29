%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_stream.erl - Handler for one ongoing ecql stream
%%==============================================================================
-module(ecql_decoding_3_8).
-compile(native).
-compile(inline).
-compile({inline_size,   500}).    %% default=24
-compile({inline_effort, 500}).   %% default=150
-compile({inline_unroll, 5}).

-include("common.hrl").

%%------------------------------------------------------------------------------
readn(N, Body, Fun) ->
  do_readn(N, Body, Fun, [])
.
do_readn(0, Body, _Fun, Ret) ->
  {lists:reverse(Ret), Body}
;
do_readn(N, Body, Fun, Ret) ->
   {Value, Rest0} = Fun(Body),
   do_readn(N-1, Rest0, Fun, [Value | Ret])
.


%%------------------------------------------------------------------------------
read_rows(<<RowCount:?T_INT32, Body/binary>>, ColTypes) ->
   TLen = length(ColTypes)
  ,{Cells, Rest} = read_n_bytes(RowCount*TLen, Body, list_to_tuple(ColTypes))
  ,{convert_rows(TLen, TLen, Cells, [], []), Rest}
.

%%------------------------------------------------------------------------------
convert_rows(_, _, [], [], Rows) ->
  lists:reverse(Rows)
;
convert_rows(0, TLen, Cells, Row, Rows) ->
  convert_rows(TLen, TLen, Cells, [], [lists:reverse(Row) | Rows])
;
convert_rows(Len, TLen, [Value | Cells], Row, Rows) ->
  convert_rows(Len-1, TLen, Cells, [Value | Row], Rows)
.

%%------------------------------------------------------------------------------
read_n_bytes(N, Body, ColTypes) ->
  do_read_n_bytes(N, Body, [], ColTypes)
.
do_read_n_bytes(0, Body, Ret, _ColTypes) ->
  {lists:reverse(Ret), Body}
;
do_read_n_bytes(N, Body, Ret, ColTypes) ->
  {V, R} = read_bytes(Body),
  Type = element((N rem tuple_size(ColTypes)) + 1, ColTypes),
  do_read_n_bytes(N-1, R, [convert(Type, V) | Ret], ColTypes)
.

%%==============================================================================
%% END OF FILE

