# ecql - Cassandra driver for Erlang.

A driver for cassandras native protocol. This driver uses cassandras stream
feature to create independent worker processes and avoid deadlocking.
Rather than exposing all features of cassandra ease of use and performance are
the main goals of this driver.
Additionally this driver includes a drop-in replacement for mnesia to make
a transition from mnesia to cassandra easier.


## Usage

To play with the driver in interactive mode there is a shell script. This script will start Cassandra in a container and run connect erlang to.
`$ ./interactive.sh`

Then you can play around on the interactive erlang shell:
```
Table = "CREATE TABLE IF NOT EXISTS key_value (key int PRIMARY KEY, value text);".
ecql:execute(Table).

% These three lines:
ecql:execute_async("INSERT INTO key_value (key, value) VALUES(?, ?)", [1, "a number"]).
ecql:execute_async("INSERT INTO key_value (key, value) VALUES(?, ?)", [42, "The answer to life, the universe and everything"]).
ecql:sync().

% Do the same as this:
ecql:execute_batch("INSERT INTO key_value (key, value) VALUES(?, ?)", [
  [1, "a number"],
  [42, "The answer to life, the universe and everything"]
]).
```

Let's insert some data and aggregate numbers

```
Table = "CREATE TABLE IF NOT EXISTS key_value (key int PRIMARY KEY, value text);".
ecql:execute(Table).

Sequence = [[Idx, io_lib:format("This is ~p", [Idx])] || Idx <- lists:seq(1, 50000)].
ecql:execute_batch("INSERT INTO key_value (key, value) VALUES(?, ?)", Sequence).


% Calculate the sum of all keys on the server side:
Sum = ecql:select_value("SELECT SUM(key) FROM key_value").

% Or the sum of products on the client side:
ProdSum = ecql:foldl(
  fun([Idx], Acc) ->
    Acc + (Idx*Idx)
  end,
  0,
  "SELECT key FROM key_value"
).
```

## ecql:execute/1,2,3
* `ecql:execute(Cql, Arguments = [], Consistency = local_quorum)`

Executes a CQL statement and returns the results if any, otherwise ok or an error message. execute/1,2,3 always blocks until the execution is done.

Returns:
* `ok` - for INSERT and CREATE statements
* `{[ColumnName], [Row]}` - for select statements
* `{error, Code, Reason}` - for any error. Code and Reason come from Cassandra error codes.


```
% INSERT
ok = ecql:execute("INSERT INTO key_value (key, value) (?, ?)", [0, "zero"]).

% SELECT
{[key, value], [0, <<"zero">>]} = ecql:execute("SELECT * FROM key_value WHERE key = 0").

% ERROR
{error,8704,"Undefined column name a"} = ecql:execute("INSERT INTO key_value (a, b) (?, ?)", [0, "zero"]).
```
CQL is any valid Cassandra Query Language statement. See here for CQL documentation: http://cassandra.apache.org/doc/cql3/CQL.html

### Prepared statements
All statements with more than one argument are **automatically  prepared internally** and the prepared statements are  cached and reused for following caches.

### Argument casting
Arguments are the arguments to be replaced in the CQL string. Arguments are being cast into the right type as much as possible. Most mentionable the Cassandra type **'text' accepts [iolists](http://www.erlangpatterns.org/iolist.html) is always returned as a erlang binary**

Example:
```
ok = ecql:execute(
  "INSERT INTO key_value (key, value) VALUES(?, ?)",
  [0, ["deep iolists", [" can", $l, $o, $o, $k], " strange ", [2], " erlang newcomers"]]
).

BinaryString = ecql:select_value("SELECT value FROM key_value WHERE key = 0").
io:format("~s ~n", [BinaryString]).
```


Consistency is the [Cassandra consistency level](https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cqlshConsistency.html). The valid atom values are:

* `default` - Alias for local_quorum and default value when no consistency is provied.
* `one` - Consistency level one.
* `two` - Consistency level two.
* `three` - Consistency level three.
* `quorum` - Consistency level quorum.
* `all` - Consistency level quorum.
* `local_quorum` - Consistency level local quorum.
* `each_quorum` - Consistency level each quorum.
* `serial` - Consistency level serial.
* `local_serial` - Consistency level local serial.
* `local_one` - Consistency level local one.

## ecql:select/1,2,3
* `ecql:select(Cql, Arguments = [], Consistency = local_quorum)`

Alias for `execute/1,2,3` - there are no behavioural differences.

## ecql:execute_async/1,2,3
* `ecql:execute_async(Cql, Arguments = [], Consistency = local_quorum)`

Returns: `ok`

Same as `ecql:execute/1,2,3` but asynchronous. It does not wait for the return value. Internally the number of running asynchronous calls is counted and capped to 100 per calling process. If the number exceeds this limit the calling process is blocked until at least one asynchronous query finishes.

## ecql:sync/0
Waits for all `execute_async` asynchronous queries of the current process to finish. This is useful when placed behind a bulk of queries.

## ecql:batch/2,3,4
* `execute_batch(Cql, ArgumentListList, Type = unlogged, Consistency = local_quorum)`

Type can be any atom of:
* `unlogged`
* `logged`

Executes a [cassandra batch statement](https://docs.datastax.com/en/cql/3.3/cql/cql_using/useBatch.html). The batch is limited to only a single CQL statement but with multiple inputs. This can be used to delete, update or insert many entries at once.

Example:
```
ArgumentListList = [[Id] || Id <- lists:seq(1, 50000)].
ecql:execute_batch("DELETE FROM key_value WHERE key = ?", ArgumentListList).
```

## ecql:select_value/1,2,3
* `select_value(Cql, Arguments = [], Consistency = local_quorum)`

Returns: `Value`

Sugar around `execute/1,2,3` for select statements. It returns only a single value from the query. Errors in the query or `ok` results from `execute/1,2,3` result in a raised exception.

Example:

```
62> ecql:select_value("SELECT value FROM key_value WHERE key = 0").
<<"deep iolists canlook strange "...>>

63> ecql:select_value("INSERT INTO key_value (key, value) VALUES(-1, 'la')").
** exception error: no match of right hand side value ok
     in function  ecql:select_column/4 (src/ecql.erl, line 467)
     in call from ecql:select_value/3 (src/ecql.erl, line 420)
```

## ecql:select_column/1,2,3
* `select_column(Cql, ColumnNumber = 1, Arguments = [], Consistency = local_quorum)`

Returns: `[Value]`

Sugar around `execute/1,2,3` for select statements. It returns only a list with a single value from every row in the query. This is useful when querying for just a single field in a table. Errors in the query or `ok` results from `execute/1,2,3` result in a raised exception.

Example:

```
68> ecql:select_column("SELECT key FROM key_value LIMIT 100").
[4317,35262,25269,39433,3372,37032,48451,14340,18417,1584,
 7034,24299,13909,40239,28386,47076,9892,41114,34323,35243,
 43690,16096,26713,19221,28459,9640,23912,46348,40244|...]
```


# Streaming functions
The driver does use Cassandra result pagination automatically internally and exposes this feature to allow stream processing of large amounts of data without loading them into memory with the following functions:

## ecql:foldl/3,4,5
* `ecql:foldl(Fun, Acc, Cql, Arguments = [], Consistency = local_quorum)`

`Fun = fun([Column1, Column2], Acc) -> NewAcc`

Returns: `NewAcc`

Extremely scalable method to process huge result sets. Like [lists:foldl](https://www.proctor-it.com/erlang-thursday-lists-foldl-3-and-lists-foldr-3/) the given function `Fun` is executed for each row of the query result. The benefit of using this function in the driver directly instead of loading the whole list first with `ecql:execute/1,2,3` is two fold:

1. The execution can start earlier and finish faster. Cassandra will send the first 1000 rows of a query as soon as they are ready and then continue sending incrementally.
1. Huge cassandra tables can be processed this way without timeouts or running out of memory on the client side.

Example:
```
ProdSum = ecql:foldl(
  fun([Idx], Acc) ->
    Acc + (Idx*Idx)
  end,
  0,
  "SELECT key FROM key_value"
).
```

## ecql:foldl_page/3,4,5
* `ecql:foldl_page(Fun, Acc, Cql, Arguments = [], Consistency = local_quorum)`

`Fun = fun([ColumnName] ,[Row], Acc) -> NewAcc`

Like `ecql:foldl/3,4,5` but passes in a whole page of ~1000 rows (size is not guaranteed) as well as the result header columns. Might be useful when trying to forward larger blocks of data e.g. to sockets or to nifs for further processing.

Example:
```
ProdSum = ecql:foldl_page(
  fun([key], Rows, Acc) ->
    lists:foldl(fun([Idx], A) ->
      A + (Idx*Idx)
    end, Acc, Rows)
  end,
  0,
  "SELECT key FROM key_value"
).
```

## ecql:foreach/2,3,4
* `ecql:foreach(Fun, Cql, Arguments = [], Consistency = local_quorum)`

Returns: `ok`

Extremely scalable way to iterate all rows of a huge query result. Similiar to `lists:foreach` executes the given Fun for each row of the result.

Example:
```
ecql:foreach(fun([Key, Value]) ->
  io:format("key: '~p' => '~p'~n", [Key, Value])
end, "SELECT * FROM key_value LIMIT 10").
```

## ecql:select_firstpage/1,2,3
* `ecql:select_firstpage(Cql, Arguments = [], Consistency = local_quorum)`

Returns: `{Page, Continuation}`

`Page = {[ColumnName], Rows}`
`Continuation = $end_of_table | tuple`

Low level function that is used to implement the other high level streaming functions. The the call returns a first page of results and a continuation. The continuation can be used to fetch further pages using `ecql:select_nextpage/1`.

Erlang shell example:
```
% Getting the first page
{Page1, Cont1} = ecql:select_firstpage("SELECT * FROM key_value").

case Cont1 of
  '$end_of_table' ->
    "one page!";
  _ ->
    {Page2, Cont2} = ecql:select_nextpage(Cont1),
    case Cont2 of
      '$end_of_table' ->
        "two pages!";
      _ ->
        {Page3, Cont3} = ecql:select_nextpage(Cont2),
        "three or more pages!"
    end
end.
% and so on

```

## ecql:select_nextpage/1,2,3
* `ecql:select_nextpage(Continuation)`

Returns: `{Page, Continuation}`

`Page = {[ColumnName], Rows}`
`Continuation = $end_of_table | tuple`

This function can only be used in conjunction with `ecql:select_firstpage/1,2,3`. Please see it's documentation and example.

# Additional Functions

## term_to_bin/1 & bin_to_term/1
* `Binary = ecql:term_to_bin(Term)`
* `Term = ecql:bin_to_term(Binary)`

These functions are usefull to store arbitary terms in Cassandra. term_to_bin is using compression and is deterministic - so it can also be used for equals queries.

Example:
```
Table = "CREATE TABLE IF NOT EXISTS term_kv (key blob PRIMARY KEY, value blob);".
ecql:execute(Table).

ecql:execute_batch("INSERT INTO term_kv (key, value) VALUES(?, ?)", [
  [ecql:term_to_bin({die_hard, 1}), ecql:term_to_bin(#{good_guy => john, bad_guy => hans})],
  [ecql:term_to_bin({die_hard, 2}), ecql:term_to_bin(#{good_guy => john, bad_guy => ramon})],
  [ecql:term_to_bin({die_hard, 3}), ecql:term_to_bin(#{good_guy => john, bad_guy => simon})],
  [ecql:term_to_bin({die_hard, 4}), ecql:term_to_bin(#{good_guy => john, bad_guy => thomas})],
  [ecql:term_to_bin({die_hard, 5}), ecql:term_to_bin(#{good_guy => john, bad_guy => yuri})]
]).

% Using tuple {die_hard, 4} to search
#{bad_guy := Name} = ecql:bin_to_term(
  ecql:select_value("SELECT value FROM term_kv WHERE key = ?",[
    ecql:term_to_bin({die_hard, 4})
  ])
).

io:format("The bad guy is: ~p~n", [Name]).
```

## ecql:release/0
* `ecql:release()`

Returns: `ok`

Releases the currently attached stream from the process. Usefull when it's known that this process is not going to do queries in the near future.

Background: Streams are currently a limited resource in the driver whose number can be configured and defaults to `100` per Cassandra host. Each stream is an independent Erlang process and can operate isolated and asynchronously from all other stream processes. The communication between a stream process and the client making queries is direct with no mediator after the initial handshake. The stream will be automatically released and returned to the pool once the client process terminates.

All of this usually happens in the background with the initial link between a client process and a stream process being established on the first query. In some cases it might be useful though to release the stream process even though the client process has not yet terminated.

# Configuration Parameters

These can be configured from the application configuration file following OTP standards or can be set before starting the driver app with `application:set_env(ecql, user, "rompompel")`

| Setting                | default            | Description                     |
| ---------------------- |:------------------:| -------------------------------:|
| cache_size             | `1000000`          | ecql_mnesia: Number of bytes used for the ecql_mnesia write-through cache |
| cluster_module         | `erlang`           | ecql_mnesia: Cluster module used for `nodes()` call to find other nodes |
| log                    | `disabled`         | what?                           |
| user                   | `"cassandra"`      | Cassandra Auth Username         |
| pass                   | `"cassandra"`      | Cassandra Auth Password         |
| hosts                  | `[{{127,0,0,1},9042}]` | List of hosts to connect to |
| keyspace               | `"ecql"`           | Keyspace to be used by all connections & streams |
| replication_strategy   | `"SimpleStrategy"` | Replication strategy to be used if keyspace does not yet exists and needs to be created. |
| replication_factor     | `2`                | Replication factor to be used if keyspace does not yet exists and needs to be created. |
| streams_per_connection | `100`              | Streams to open for every Connection |
| connections_per_host   | `1`                | Connections to open to each Server in the cluster |

# Todo List

* Make streams_per_connection dynamic!!
* Document performance numbers vs. other drivers
* SSL
* Replace encode function with nif for performance.
* Read configuration from env
* Token aware routing
* Support multiple keyspaces & clusters
