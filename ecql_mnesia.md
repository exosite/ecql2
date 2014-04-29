ecql_mnesia - Mnesia compatibility layer for Cassandra
=========================================================

Gap-fill replacement for mnesia to make a transition from mnesia to cassandra 
easier.

Current Limitations
-------------------

* Can't have underscores _ in records
* All but trivial match statements are not-efficient
* No Transactions! ecql_mnesia:transaction(fun()) working but not in a transaction.
* No QLC support.

