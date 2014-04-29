#!/bin/bash
name=cassandra_ecql
ip=`docker inspect --format='{{ .NetworkSettings.IPAddress }}' $name`

if [ "$ip" = "" ] 
then
  docker run --rm --name $name -m 2g -d cassandra:3.11.2
  ip=`docker inspect --format='{{ .NetworkSettings.IPAddress }}' $name`
fi

if [ "$ip" != "" ] 
then
  limit=180
  tries=0
  notyet=1
  while [[ $notyet -ge 1 ]]; do
    if [[ $tries -ge $limit ]]; then
      echo "failed"
      exit 1
    fi
    
    nc -z $ip 9042;
    notyet=$?
    sleep 1

    tries=$((tries + 1))
  done
  echo $ip
  exit 0
fi

exit 1
