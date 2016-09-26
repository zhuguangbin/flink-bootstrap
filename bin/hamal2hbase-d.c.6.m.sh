#!/usr/bin/env bash
set -e
export HADOOP_CONF_DIR=/etc/hadoop/conf
export FLINK_HOME=/opt/flink-1.1.2
export FLINKRUNNER=$FLINK_HOME/bin/flink

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"
JOBJAR=`ls "$FWDIR"/lib/flink-demo*.jar`
export CLASS=com.mvad.flink.demo.streaming.Hamal2HBase
export CONSUMERGROUP=Hamal2HBase
export OUTPUT_HTABLENAME=dspsession

export TOPIC=d.c.6.m
export QUEUE=etl
export JOBNAME=Flink:[${QUEUE}][Hamal2HBase][${TOPIC}]
export NUM_CONTAINERS=15

$FLINKRUNNER run -m yarn-cluster -ynm ${JOBNAME} -yqu ${QUEUE} -yn ${NUM_CONTAINERS} -yjm 2048 -ytm 4096 \
-c ${CLASS} ${JOBJAR} \
--topic ${TOPIC} \
--bootstrap.servers kf4ss.prod.mediav.com:9092,kf5ss.prod.mediav.com:9092,kf6ss.prod.mediav.com:9092 \
--group.id ${CONSUMERGROUP} \
--htablename ${OUTPUT_HTABLENAME} 
