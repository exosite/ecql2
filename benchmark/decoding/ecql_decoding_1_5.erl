%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_stream.erl - Handler for one ongoing ecql stream
%%==============================================================================
-module(ecql_decoding_1_5).
-compile(native).
-compile({hipe,[o3]}).
%~ -compile(inline).
%~ -compile({inline_size,   500}).    %% default=24
%~ -compile({inline_effort, 500}).   %% default=150
%~ -compile({inline_unroll, 5}).

-include("common.hrl").

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
read_rows(<<RowCount:?T_INT32, Body/binary>>, ColTypes) ->
   TLen = length(ColTypes)
  ,{Cells, Rest} = read_n_bytes(RowCount*TLen, Body)
  ,{convert_rows(ColTypes, RowCount, Cells), Rest}
.

%%------------------------------------------------------------------------------
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

%%==============================================================================
%% END OF FILE

