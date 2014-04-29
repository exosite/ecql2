%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_stream.erl - Handler for one ongoing ecql stream
%%==============================================================================
-module(ecql_stream_3500).

-export([wire_batch/4]).

%% Includes
-include("../../include/ecql.hrl").

%%------------------------------------------------------------------------------
zipwith_wire([], [], Ret) ->
  lists:reverse(Ret)
;
zipwith_wire([T1 | T], [V1 | V], Ret) ->
  zipwith_wire(T, V, [wire_longstring(wire_value(T1, V1)) | Ret])
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
    ,zipwith_wire(RequestTypes, Args, [])
    |do_wire_batch(Head, ListOfArgs, Consistency, RequestTypes)
  ]
;
do_wire_batch(_Head, [], Consistency, _RequestTypes) ->
  [<<Consistency:?T_UINT16>>]
.

%%------------------------------------------------------------------------------
% 0x0001    Ascii
wire_value(1, Value) ->
  Value
;
% 0x0002    Bigint
wire_value(2, Value) ->
  wire_bigint(Value)
;
% 0x0003    Blob
wire_value(3, Value) ->
  Value
;
% 0x0004    Boolean
% NOPE
% 0x0005    Counter
wire_value(5, Value) ->
  wire_bigint(Value)
;
% 0x0006    Decimal
% NOPE
% 0x0007    Double
% NOPE
% 0x0008    Float
% NOPE
% 0x0009    Int
wire_value(9, Value) ->
  wire_int(Value)
;
% 0x000B    Timestamp
wire_value(11, Value) ->
  wire_int(Value)
;
% NOPE
% 0x000C    Uuid
% NOPE
% 0x000D    Varchar
wire_value(13, Value) ->
  Value
;
% 0x000E    Varint
wire_value(14, Value) ->
  wire_bigint(Value)
;
% 0x000F    Timeuuid
% NOPE
% 0x0010    Inet
% NOPE
% 0x0020    List: the value is an [option], representing the type
%                of the elements of the list.
wire_value({list, ValueType}, Values) when is_list(Values) ->
  wire_shortlist(lists:map(fun(Value) ->
    wire_shortstring(wire_value(ValueType, Value))
  end, Values))
;
wire_value({list, ValueType}, Value) ->
  wire_value({list, ValueType}, [Value])
;
% 0x0021    Map: the value is two [option], representing the types of the
%               keys and values of the map
wire_value({map, {KeyType, ValueType}}, Values) when is_list(Values) ->
  wire_shortlist(lists:map(fun({Key, Value}) -> [
     wire_shortstring(wire_value(KeyType, Key))
    ,wire_shortstring(wire_value(ValueType, Value))
  ] end, Values))
;
wire_value({map, ValueType}, Value) when is_tuple(Value) ->
  wire_value({map, ValueType}, [Value])
;
% 0x0022    Set: the value is an [option], representing the type
%                of the elements of the set
wire_value({set, ValueType}, Value) ->
  wire_value({list, ValueType}, Value)
.

%%------------------------------------------------------------------------------
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
wire_longstring(Value) when is_binary(Value) ->
  <<(size(Value)):?T_INT32, Value/binary>>
;
wire_longstring(Value) when is_list(Value) ->
  [<<(iolist_size(Value)):?T_INT32>>, Value]
;
wire_longstring(Value) when is_atom(Value) ->
  wire_longstring(atom_to_binary(Value, utf8))
.

%%------------------------------------------------------------------------------
wire_shortlist(Value) ->
  [<<(length(Value)):?T_UINT16>>, Value]
.

%%------------------------------------------------------------------------------
wire_shortstring(Value) ->
  [<<(iolist_size(Value)):?T_UINT16>>, Value]
.

%%==============================================================================
%% END OF FILE
