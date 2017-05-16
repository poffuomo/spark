#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Shell script to stop all spark daemons, including the additional workers dynamically added.
# This is different from the script 'stop-all.sh' that actually stops only the master and the
# slaves that were created "statically" (i.e. not with 'start-additional-slave.sh').
# Run this on the master node.

# Usage: stop-really-all.sh
#   Stops the master and all the existing

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

# Load the Spark configuration
. "${SPARK_HOME}/sbin/spark-config.sh"

# Stop the slaves, then the master
"${SPARK_HOME}/sbin"/stop-additional-slaves.sh
"${SPARK_HOME}/sbin"/stop-master.sh

if [ "$1" == "--wait" ]
then
  printf "Waiting for workers to shut down..."
  while true
  do
    running=`${SPARK_HOME}/sbin/slaves.sh ps -ef | grep -v grep | grep deploy.worker.Worker`
    if [ -z "$running" ]
    then
      printf "\nAll workers (really, all) successfully shut down.\n"
      break
    else
      printf "."
      sleep 10
    fi
  done
fi
