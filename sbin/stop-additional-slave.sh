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

# Shell script to stop additional workers on a single slave on the machine this script is executed
# on. This is useful e.g. for faking the dynamic allocation of executors in Spark local deployment
# mode.
# Depending on the optional value of SPARK_WORKER_INSTANCES environmental variable, the number of
# "additional" workers that will be stopped in the machine will be wither 1 or the value
# specified in SPARK_WORKER_INSTANCES.
#
# Environment variables
#
#   SPARK_WORKER_INSTANCES The number of worker instances to stop among all the workers
#                          running on this slave for every execution of the script.
#                          Default is 1.

# Usage: stop-additional-slave.sh
#   Stops some workers on this worker machine. The input argument gives the number of workers that
#	will be stopped

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${SPARK_HOME}/sbin/spark-config.sh"

. "${SPARK_HOME}/bin/load-spark-env.sh"

# Count the number of Spark workers already running on this machine
EXISTING_WORKER_INSTANCES=$(ps -ef | grep -v grep | grep deploy.worker.Worker | wc -l)

if [ "$SPARK_WORKER_INSTANCES" = "" ]; then
  "${SPARK_HOME}/sbin"/spark-daemon.sh stop org.apache.spark.deploy.worker.Worker $EXISTING_WORKER_INSTANCES
else
  # Stop a number of slaves given by the variable SPARK_WORKER_INSTANCES
  for ((i=0; i<$SPARK_WORKER_INSTANCES; i++)); do
    "${SPARK_HOME}/sbin"/spark-daemon.sh \
      stop org.apache.spark.deploy.worker.Worker $(( $EXISTING_WORKER_INSTANCES - $i ))
  done
fi
