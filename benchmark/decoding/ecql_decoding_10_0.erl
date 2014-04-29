%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_stream.erl - Handler for one ongoing ecql stream
%%==============================================================================
-module(ecql_decoding_10_0).

-include("common.hrl").

%%------------------------------------------------------------------------------
readn(0, Body, _Fun) ->
  {[], Body}
;
readn(N, Body, Fun) ->
   {Value, Rest0} = Fun(Body)
  ,{Values, Rest1} = readn(N-1, Rest0, Fun)
  ,{[Value | Values], Rest1}
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

