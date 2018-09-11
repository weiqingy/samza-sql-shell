#!/bin/bash

if [ `uname` == 'Linux' ];
then
  base_dir=$(readlink -f $(dirname $0))
else
  base_dir=$(dirname $0)
fi

if [ "x$HEAP_OPTS" = "x" ]; then
    export HEAP_OPTS="-Xmx4G -Xms4G"
fi

EXTRA_ARGS="-name samza-shell -loggc"

exec $base_dir/run-class.sh $EXTRA_ARGS org.apache.samza.tools.client.cli.Main "$@"
