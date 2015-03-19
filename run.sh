#!/bin/bash

mvn clean package

class=$1
args1=$2

if [ ! $class ]; then
  echo "args must be more than one"
  exit 0
fi

# if [ ! $args1 ]; then
#  args1=/user/hadoop/wordcount/dual.sql
#  $2=$args1
# fi

echo "hadoop jar *.jar $@"
echo "*********************************************************************"
/Users/Sabo/Developer/hadoop-1.0.1/bin/hadoop jar /Users/Sabo/tiny/Documents/workspace/wordcount/target/wordcount-0.0.1-SNAPSHOT.jar "$@"
