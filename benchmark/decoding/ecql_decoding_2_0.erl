%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_stream.erl - Handler for one ongoing ecql stream
%%==============================================================================
-module(ecql_decoding_2_0).
%~ -compile(inline).
%~ -compile({inline_size,   52}).    %% default=24
-on_load(init/0).

-include("common.hrl").

%%------------------------------------------------------------------------------
init() ->
  (erlang:load_nif("./ecql_decoding", 0) == ok) orelse begin
    ok = erlang:load_nif("../benchmark/decoding/ecql_decoding", 0)
  end,
  ok
.

%%------------------------------------------------------------------------------
read_rows(_, _) ->
  exit(nif_library_not_loaded)
.

%%------------------------------------------------------------------------------
readn(0, Body, _Fun) ->
  {[], Body}
;
readn(N, Body, Fun) ->
   {Value, Rest0} = Fun(Body)
  ,{Values, Rest1} = readn(N-1, Rest0, Fun)
  ,{[Value | Values], Rest1}
.

%%==============================================================================
%% END OF FILE

