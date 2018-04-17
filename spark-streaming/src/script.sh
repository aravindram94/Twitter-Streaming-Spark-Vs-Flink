##!/usr/bin/env bash
#
#export SPARK_HOME="../../spark-2.2.1-bin-hadoop2.7"
#
#PROG="twitter-stream-analyser.py"
#
#PACKAGES="org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,anguenot/pyspark-cassandra:0.7.0"
#
#${SPARK_HOME}/bin/spark-submit --packages ${PACKAGES} ${PROG} localhost:9092 tweets

#!/usr/bin/env bash

export SPARK_HOME="../../spark-2.3.0-bin-hadoop2.7"

PROG="twitter-structured-stream-analyser.py"

PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0"

${SPARK_HOME}/bin/spark-submit --packages ${PACKAGES} ${PROG} localhost:9092 tweets