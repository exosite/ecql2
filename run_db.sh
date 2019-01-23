#!/bin/bash
mkdir -p ./eunit && ./test/ensure_cassandra.sh > .eunit/cassandra.ip
