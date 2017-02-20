#!/bin/bash

set -e

DDB_PATH=$HOME/local/dynamodb
DDB_LOG=$DDB_PATH/dynamodb.log

if [ $# != 1 ]; then
  printf "usage: $0 start|stop\n" 1>&2
  exit 1
fi

if [ $1 = 'start' ]; then
	nohup java -Djava.library.path=$DDB_PATH/DynamoDBLocal_lib -jar $DDB_PATH/DynamoDBLocal.jar -sharedDb >$DDB_LOG 2>&1 &
elif [ $1 = 'stop' ]; then
	pkill -f DynamoDBLocal.jar
fi
