#!/usr/bin/env bash
set -e
export HADOOP_CONF_DIR=/etc/hadoop/conf
export FLINK_HOME=/opt/flink-1.1.2
export FLINKRUNNER=$FLINK_HOME/bin/flink

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"
JOBJAR=`ls "$FWDIR"/lib/flink-demo*.jar`
export CLASS=com.mvad.flink.demo.streaming.Hamal2HDFS
export CONSUMERGROUP=Hamal2HDFS
export OUTPUT_HDFSDIR=/tmp/rawlog/dsp

export TOPIC=d.s.6
export QUEUE=etl
export JOBNAME=Flink:[${QUEUE}][Hamal2HDFS][${TOPIC}]
export NUM_CONTAINERS=100

$FLINKRUNNER run -m yarn-cluster -ynm ${JOBNAME} -yqu ${QUEUE} -yn ${NUM_CONTAINERS} -yjm 2048 -ytm 4096 \
-c ${CLASS} ${JOBJAR} \
--topic ${TOPIC} \
--bootstrap.servers kf4ss.prod.mediav.com:9092,kf5ss.prod.mediav.com:9092,kf6ss.prod.mediav.com:9092 \
--group.id ${CONSUMERGROUP} \
--hdfsoutdir ${OUTPUT_HDFSDIR}