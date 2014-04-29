%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_stream.erl - Handler for one ongoing ecql stream
%%==============================================================================
-module(ecql_decoding_7_0).
-compile(native).

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
  readn(RowCount, Body, fun(BinRow) ->
     {Row, RowRest} = readn(length(ColTypes), BinRow, fun read_bytes/1)
    ,{lists:zipwith(fun convert/2, ColTypes, Row), RowRest}
  end)
.

%%==============================================================================
%% END OF FILE

