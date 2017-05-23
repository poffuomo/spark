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

# Shell script to stop all the workers including the "additional" ones on a single slave on the
# machine this script is executed on. This is useful e.g. for faking the dynamic allocation
# of executors in Spark local deployment mode.
# This is different from the script 'stop-additional-slave.sh' which only stops 1 or
# SPARK_WORKER_INSTANCES workers per execution among all the workers on this machine.
# This is also different from the script 'stop-all.sh' which only stops the "non-additional"
# workers but leaves the dynamically added ones (i.e. the workers created with
# 'start-additional-slave.sh').

# Usage: stop-additional-slaves.sh
#   Stops all the workers on this worker machine

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${SPARK_HOME}/sbin/spark-config.sh"

. "${SPARK_HOME}/bin/load-spark-env.sh"

# Count the number of Spark workers already running on this machine
EXISTING_WORKER_INSTANCES=$(ps -ef | grep -v grep | grep deploy.worker.Worker | wc -l)

# Stop all the slaves, the "traditional" ones and the dynamically added ones
for ((i=0; i<$EXISTING_WORKER_INSTANCES; i++)); do
  "${SPARK_HOME}/sbin"/spark-daemon.sh \
    stop org.apache.spark.deploy.worker.Worker $(( $EXISTING_WORKER_INSTANCES - $i ))
done
