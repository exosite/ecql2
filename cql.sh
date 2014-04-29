#!/bin/bash
./test/ensure_cassandra.sh
docker exec -ti cassandra_ecql cqlsh
