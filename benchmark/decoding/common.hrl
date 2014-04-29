-include("../include/ecql.hrl").

%% Public API
-export([
   read_rows/2
]).

%% Unused function suppress
-export([
   read_sbytes/1,
   read_bytes/1,
   convert/2,
   convert_float/1,
   convert_int/1
]).


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
convert(11, Value) ->
  convert_int(Value)
;
% 0x000C    Uuid
convert(12, Value) ->
  Value
;
% NOPE
% 0x000D    Varchar
convert(13, Value) ->
  % Change for Elixir string as binary semantic
  % binary_to_list(Value)
  Value
;
% 0x000E    Varint
convert(14, Value) ->
  convert_int(Value)
;
% 0x000F    Timeuuid
% NOPE
% 0x0010    Inet
% NOPE
% 0x0013    Smallint
convert(19, Value) ->
  convert_int(Value)
;
% 0x0014    Tinyint
convert(20, Value) ->
  convert_int(Value)
;
% 0x0020    List: the value is an [option], representing the type
%                of the elements of the list.
convert({list, ValueType}, <<Count:?T_UINT16, Body/binary>>) ->
   {Values, <<>>} = readn(Count, Body, fun read_sbytes/1)
  ,lists:map(fun(Value) -> convert(ValueType, Value) end, Values)
;
% 0x0021    Map: the value is two [option], representing the types of the
%               keys and values of the map
convert({map, {KeyType, ValueType}}, <<Count:?T_UINT16, Body/binary>>) ->
  {Values, <<>>} = readn(Count, Body, fun(BinRow) ->
     {[Key, Value], RowRest} = readn(2, BinRow, fun read_sbytes/1)
    ,{{convert(KeyType, Key), convert(ValueType, Value)}, RowRest}
  end)
  ,Values
;
% 0x0022    Set: the value is an [option], representing the type
%                of the elements of the set
convert({set, ValueType}, Binary) ->
  convert({list, ValueType}, Binary)
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

