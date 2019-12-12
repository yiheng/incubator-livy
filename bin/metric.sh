#!/bin/bash

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
# Runs Livy server.

# Usage: ./metric.sh LOG_FILE DATE METRIC_FOLDER
# For example, you can retrieve metric on 2019-11-11 by
#        ./metric.sh livy-server.log 2019-11-11
# you can retrieve metric on yesterday by
#        ./metric.sh livy-server.log yesterday

FILE=$1
DATE=`date --date="$2" +%Y-%m-%d`
METRIC_FOLDER=$3

echo "Log file is $FILE, Find log on $DATE"

SESSION_COUNT=`cat $FILE | grep 'LineBufferedStream:39.*user:' | grep ^$DATE | wc -l`
USER_COUNT=`cat $FILE | grep 'LineBufferedStream:39.*user:' | grep ^$DATE | awk '{print $7}' | sort | uniq | wc -l`
USER_LIST=`cat $FILE | grep 'LineBufferedStream:39.*user:' | grep ^$DATE | awk '{print $7}' | sort | uniq`
STATEMENT_COUNT=`cat $FILE | grep 'executing SQL query' | grep ^$DATE | wc -l`

echo "Session count is $SESSION_COUNT"
echo "Statement count is $STATEMENT_COUNT"
echo "User count is $USER_COUNT"
echo "User list is:"
echo "$USER_LIST"

if [ -z $METRIC_FOLDER ]
then
    echo "Not output result to metric folder"
else
    echo "Save metric to file $METRIC_FOLDER/$DATE"
    echo "Session count is $SESSION_COUNT" > "$METRIC_FOLDER/$DATE"
    echo "Statement count is $STATEMENT_COUNT" >> "$METRIC_FOLDER/$DATE"
    echo "User count is $USER_COUNT" >> "$METRIC_FOLDER/$DATE"
    echo "User list is:" >> "$METRIC_FOLDER/$DATE"
    echo "$USER_LIST" >> "$METRIC_FOLDER/$DATE"
fi
