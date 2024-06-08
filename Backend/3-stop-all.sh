#!/bin/bash 

#STOP NEO4J
sudo service neo4j stop

#STOP SPARK
sh $SPARK_HOME/sbin/stop-all.sh

#STOP HADOOP
$HADOOP_DIR/sbin/stop-dfs.sh
$HADOOP_DIR/sbin/stop-yarn.sh
jps



