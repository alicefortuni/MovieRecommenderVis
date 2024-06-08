#!/bin/bash 

#STOP SPARK
sh $SPARK_HOME/sbin/stop-all.sh

#STOP HADOOP
$HADOOP_DIR/sbin/stop-dfs.sh
$HADOOP_DIR/sbin/stop-yarn.sh
jps

#START HADOOP
$HADOOP_DIR/sbin/start-dfs.sh
$HADOOP_DIR/sbin/start-yarn.sh
jps

#DELETE INPUT/OUTPUT DIRECTORY IN HDFS
$HADOOP_DIR/bin/hdfs dfsadmin -safemode leave
$HADOOP_DIR/bin/hdfs dfs -rm -r input
$HADOOP_DIR/bin/hdfs dfs -rm -r output
$HADOOP_DIR/bin/hdfs dfs -rm -r model


#LOAD INPUT DIRECTORY IN HDFS
$HADOOP_DIR/bin/hdfs dfs -put $MOVIE_BACKEND_DIR/dataset input

#START SPARK
sh $SPARK_HOME/sbin/start-all.sh

#START NEO4J
sudo service neo4j start

#OPEN JUPYTER NOTEBOOK
cd $MOVIE_BACKEND_DIR
jupyter notebook











