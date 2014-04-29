%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql.hrl - Header
%%==============================================================================

%% Records
-record(frame, {stream, opcode, body, flags}).

%% Defines
-ifndef(native_protocol_3).
-define(VS_REQUEST, 4).
-define(VS_RESPONSE, 132).
-else.
-define(VS_REQUEST, 3).
-define(VS_RESPONSE, 131).
-endif.

-define(OP_ERROR, 0).
-define(OP_STARTUP, 1).
-define(OP_READY, 2).
-define(OP_AUTHENTICATE, 3).
-define(OP_QUERY, 7).
-define(OP_RESULT, 8).
-define(OP_PREPARE, 9).
-define(OP_EXECUTE, 10).
-define(OP_BATCH, 13).
-define(OP_AUTH_CHALLENGE, 14).
-define(OP_AUTH_RESPONSE, 15).
-define(OP_AUTH_SUCCESS, 16).

-define(T_UINT8,   8/big-unsigned-integer).
-define(T_UINT16, 16/big-unsigned-integer).
-define(T_UINT32, 32/big-unsigned-integer).
-define(T_INT8,    8/big-signed-integer).
-define(T_INT16,  16/big-signed-integer).
-define(T_INT32,  32/big-signed-integer).
-define(T_INT64,  64/big-signed-integer).
-define(T_FLOAT,  32/big-signed-float).
-define(T_DOUBL,  64/big-signed-float).

-define(RT_VOID, 1:?T_INT32).
-define(RT_ROWS, 2:?T_INT32).
-define(RT_SETKEYSPACE, 3:?T_INT32).
-define(RT_PREPARED, 4:?T_INT32).
-define(RT_SCHEMACHANGE, 5:?T_INT32).

-define(ER_UNPREPARED ,16#2500).

-define(CL_ONE, one).
-define(CL_TWO, two).
-define(CL_THREE, three).
-define(CL_QUORUM, quorum).
-define(CL_ALL, all).
-define(CL_LOCAL_QUORUM, local_quorum).
-define(CL_EACH_QUORUM, each_quorum).
-define(CL_SERIAL, serial).
-define(CL_LOCAL_SERIAL, local_serial).
-define(CL_LOCAL_ONE, local_one).

-define(CACHE_SLICES_TUPLE, {
  ecql_cache_1, ecql_cache_2, ecql_cache_3, ecql_cache_4, ecql_cache_5
}).
-define(CACHE_SLICES_LIST, [
  ecql_cache_1, ecql_cache_2, ecql_cache_3, ecql_cache_4, ecql_cache_5
]).

%%==============================================================================
%% END OF FILE
