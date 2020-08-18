#!/bin/bash

: ${SLEEP_LENGTH:=2}

wait_for() {
  echo Waiting for $1 to listen on $2...
  while ! nc -z $1 $2; do echo sleeping; sleep $SLEEP_LENGTH; done
}

for var in "$@"
do
  host=${var%:*}
  port=${var#*:}
  wait_for $host $port
done

node_modules/.bin/nodemon -e js,yaml --inspect=0.0.0.0:5858 --ignore 'spec/*/*.spec.js' --max-old-space-size=20 --expose-gc ./bin/www