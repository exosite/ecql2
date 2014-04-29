%% -*- coding: utf-8 -*-
-module(ecql_types_unit).
-export([tests/0]).
-include_lib("eunit/include/eunit.hrl").

% Testing type support

tests() ->
  {setup, fun start/0, fun stop/1, [
    {generator, fun basic_tests/0}
  ]}
.

start() ->
  {ok, _Apps} = application:ensure_all_started(ecql)
.

stop(_) ->
  ok
.

%% DATA PROVIDER
basic_tests() ->
  [{
    setup,
    fun() -> create_table(Type) end,
    fun destroy_table/1, {
      name(Type), with, Type, [
        fun insert_read_test/1
        | advanced_tests(Type)
      ]
    }
  } || Type <- basic_types()]
.

%% TESTS START HERE
create_table({Name, _Class, _Comment}) ->
  ok = ecql:create_table(
    tablename(Name),
    do_create_table(Name)
  ),
  do_create_index(Name),
  {Name, _Class, _Comment}
.

do_create_table(frozen) ->
  "a int PRIMARY KEY, b frozen<list<int>>"
;
do_create_table(list) ->
  "a int PRIMARY KEY, b list<int>"
;
do_create_table(map) ->
  "a int PRIMARY KEY, b map<int, text>"
;
do_create_table(set) ->
  "a int PRIMARY KEY, b set<int>"
;
do_create_table(tuple) ->
  "a int PRIMARY KEY, b tuple<int,int,int>"
;
do_create_table(udt) ->
  ok = ecql:execute("CREATE TYPE IF NOT EXISTS phone (
    country_code int,
    number text)"),

  ok = ecql:execute("CREATE TYPE IF NOT EXISTS address (
    street text,
    city text,
    zip text,
    phones map<text, frozen<phone>>)"),

  ["a int PRIMARY KEY, b map<text, frozen<address>>"]
;
do_create_table(Name) ->
  ["a int PRIMARY KEY, b ", fieldname(Name)]
.

do_create_index(counter) ->
  ok
;
do_create_index(frozen) ->
  ok = ecql:create_index(indexname(frozen), tablename(frozen), "FULL(b)")
;
do_create_index(map) ->
  ok = ecql:create_index([indexname(map), "_values"], tablename(map), "VALUES(b)"),
  ok = ecql:create_index([indexname(map), "_entries"], tablename(map), "ENTRIES(b)"),
  ok = ecql:create_index([indexname(map), "_keys"], tablename(map), "KEYS(b)")
;
do_create_index(Name) ->
  ok = ecql:create_index(indexname(Name), tablename(Name), "b")
.

destroy_table({_Name, _Class, _Comment}) ->
  ok
  %~ ?assertMatch(ok, ecql:execute(
    %~ ["DROP TABLE ", tablename(Name)]
  %~ ))
.

%% TESTS
insert_read_test({map, _, _Comment}) ->
  insert_read(map, [
    #{1 => <<"a">>, 2 => <<"b">>},
    % List to binary conversion test
    {#{1 => "a", 2 => "b"}, #{1 => <<"a">>, 2 => <<"b">>}},
    % Keywordlist to map conversion test
    {[{1, "a"}, {2, "b"}], #{1 => <<"a">>, 2 => <<"b">>}}
  ])
;
insert_read_test({Name, list, _Comment}) ->
  insert_read(Name, [
    [1, 2, 3],
    [3, 2, 1]
  ])
;
insert_read_test({set, _, _Comment}) ->
  insert_read(set, [
    [1, 2, 3],
    {[1, 2, 2], [1, 2]}
  ])
;
insert_read_test({tuple, _, _Comment}) ->
  insert_read(tuple, [
    {1, 2, 3},
    {1, undefined, 3}
  ])
;
insert_read_test({udt, _, _Comment}) ->
  insert_read(udt, [
    % Nested Test
    #{<<"Home">> =>
      #{
        street => <<"street">>,
        city => <<"city">>,
        zip => <<"zip">>,
        phones => #{
          <<"master">> => #{
            country_code => 1,
            number => <<"12345">>
          }
        }
      }
    },

    % Missing country_code
    {
      #{<<"Home">> =>
        #{
          street => <<"street">>,
          city => <<"city">>,
          zip => <<"zip">>,
          phones => #{
            <<"master">> => #{
              number => <<"12345">>
            }
          }
        }
      },
      #{<<"Home">> =>
        #{
          street => <<"street">>,
          city => <<"city">>,
          zip => <<"zip">>,
          phones => #{
            <<"master">> => #{
              country_code => undefined,
              number => <<"12345">>
            }
          }
        }
      }
    }
  ])
;
insert_read_test({Name, numbers, _Comment}) ->
  insert_read(Name, [
    1, 42
  ])
;
insert_read_test({Name, float, _Comment}) ->
  insert_read(Name, [
    1.0, 42.0,
    {1, 1.0},
    {42, 42.0}
  ])
;
insert_read_test({Name, date, _Comment}) ->
  insert_read(Name, [
    {1982, 12, 16}
  ])
;
insert_read_test({Name, decimal, _Comment}) ->
  insert_read(Name, [
    {     10, {0, 10}},
    {{0, 10}, {0, 10}},
    {   10.0, {3, 10000}}
  ])
;
insert_read_test({Name, booleans, _Comment}) ->
  insert_read(Name, [
    {0, false},
    false,
    true,
    {1, true},
    {2, true},
    {atom, true}
  ])
;
insert_read_test({Name, strings, _Comment}) ->
  insert_read(Name, [
    {"hello world", <<"hello world">>},
    <<"hello world">>
  ]),

  case Name of
    ascii -> ok;
    _ ->
      insert_read(Name, [
        unicode:characters_to_binary("grüß gott 1"),
        <<"grüß gott 2"/utf8>>
      ])
    %~
  end
;
insert_read_test({Name, inet, _Comment}) ->
  insert_read(Name, [
    {127, 0, 0, 1},
    {0,0,0,0,0,0,0,1},
    {0,0,0,0,0,65535,49320,10754},
    {"192.168.42.2", {192,168,42,2}},
    {"::FFFF:192.168.42.2", {0,0,0,0,0,65535,49320,10754}},
    {<<"::FFFF:192.168.42.2">>, {0,0,0,0,0,65535,49320,10754}}
  ])
;
insert_read_test({Name, uuids, _Comment}) ->
  insert_read(Name, [
    <<"6ba7b814-9dad-11d1-80b4-00c04fd430c8">>,
    <<"6ba7b811-9dad-11d1-80b4-00c04fd430c8">>,
    {<<"6ba7b8119dad11d180b400c04fd430c8">>, <<"6ba7b811-9dad-11d1-80b4-00c04fd430c8">>},
    {"6ba7b811-9dad-11d1-80b4-00c04fd430c8", <<"6ba7b811-9dad-11d1-80b4-00c04fd430c8">>},
    {<<16#6ba7b8119dad11d180b400c04fd430c8:128>>, <<"6ba7b811-9dad-11d1-80b4-00c04fd430c8">>},
    {16#6ba7b8119dad11d180b400c04fd430c8, <<"6ba7b811-9dad-11d1-80b4-00c04fd430c8">>}
  ])
;
insert_read_test({_Atom, _Class, _Comment}) ->
  ok
.

insert_read(Name, [{Insert, Read} | Tail]) ->
  ok = ecql:execute(
    ["INSERT INTO ", tablename(Name), " (a, b) VALUES(?, ?)"],
    [1, Insert]
  ),

  Read = ecql:select_value(
    ["SELECT b FROM ", tablename(Name), " WHERE a = ?"],
    [1]
  ),

  insert_read(Name, Tail)
;
insert_read(Name, [Insert | Tail]) ->
  insert_read(Name, [{Insert, Insert} | Tail])
;
insert_read(_Name, []) ->
  ok
.



advanced_tests({_Name, _Class, _Comment}) ->
  []
.


%% HELPERS

fieldname(Name) -> atom_to_list(Name).
tablename(Name) -> [atom_to_list(Name), "_table"].
indexname(Name) -> [tablename(Name), "_index"].
name({Name, _Class, _Comment}) -> atom_to_list(Name).

basic_types() -> [
  % Type, Class, Description
  {ascii, strings, "US-ASCII character string"},
  {bigint, numbers, "64-bit signed long"},
  {blob, blobs, "Arbitrary bytes (no validation), expressed as hexadecimal"},
  {boolean, booleans, "true or false"},
  {date, date, "Value is a date with no corresponding time value; Cassandra encodes date as a 32-bit integer representing days since epoch (January 1, 1970). Dates can be represented in queries and inserts as a string, such as 2015-05-03 (yyyy-mm-dd)"},
  {decimal, decimal, "floats, Variable-precision decimal"},
  {double, float, "floats, 64-bit IEEE-754 floating point"},
  {float, float, "floats, 32-bit IEEE-754 floating point"},
  {inet, inet, "IP address string in IPv4 or IPv6 format, used by the python-cql driver and CQL native protocols"},
  {int, numbers, "32-bit signed integer"},
  {smallint, numbers, "2 byte integer"},
  {text, strings, "UTF-8 encoded string"},
  {timestamp, numbers, "strings, Date and time with millisecond precision, encoded as 8 bytes since epoch. Can be represented as a string, such as 2015-05-03 13:30:54.234."},
  {time, numbers, "Value is encoded as a 64-bit signed integer representing the number of nanoseconds since midnight. Values can be represented as strings, such as 13:30:54.234."},
  {timeuuid, uuids, "Version 1 UUID only"},
  {tinyint, numbers, "1 byte integer"},
  {uuid, uuids, "A UUID in standard UUID format"},
  {varchar, strings, "UTF-8 encoded string"},
  {varint, numbers, "Arbitrary-precision integer Java type"},

  {counter, counter, "Distributed counter value (64-bit long)"},
  {frozen, list, "collections, tuples, A frozen value serializes multiple components into a single value. Non-frozen types allow updates to individual fields. Cassandra treats the value of a frozen type as a blob. The entire value must be overwritten. Note: Cassandra no longer requires the use of frozen for tuples: frozen <tuple <int, tuple<text, double>>>"},
  {list, list, "A collection of one or more ordered elements: [literal, literal, literal].
  CAUTION:
  Lists have limitations and specific performance considerations. Use a frozen list to decrease impact. In general, use a set instead of list."},
  {map, other, "A JSON-style array of literals: { literal : literal, literal : literal ... }"},
  {set, other, "A collection of one or more elements: { literal, literal, literal }"},
  {tuple, other, "A group of 2-3 fields."},
  {udt, other, "A group of 2-3 fields."}
].