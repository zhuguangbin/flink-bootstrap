#!/usr/bin/env bash
set -e
export HADOOP_CONF_DIR=/etc/hadoop/conf
export FLINK_HOME=/opt/flink-1.1.2
export FLINKRUNNER=$FLINK_HOME/bin/flink

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"
JOBJAR=`ls "$FWDIR"/lib/flink-demo*.jar`
export CLASS=com.mvad.flink.demo.streaming.Hamal
export CONSUMERGROUP=Hamal
export OUTPUT_HTABLENAME=dspsession
export OUTPUT_HDFSDIR=/tmp/rawlog/dsp

export TOPIC=d.u.6.m
export QUEUE=etl
export JOBNAME=Flink:[${QUEUE}][Hamal][${TOPIC}]
export NUM_CONTAINERS=400

$FLINKRUNNER run -m yarn-cluster -ynm ${JOBNAME} -yqu ${QUEUE} -yn ${NUM_CONTAINERS} -yjm 2048 -ytm 4096 \
-c ${CLASS} ${JOBJAR} \
--topic ${TOPIC} \
--bootstrap.servers kf4ss.prod.mediav.com:9092,kf5ss.prod.mediav.com:9092,kf6ss.prod.mediav.com:9092 \
--zookeeper.connect zk1ss.prod.mediav.com:2191,zk2ss.prod.mediav.com:2191,zk3ss.prod.mediav.com:2191,zk12ss.prod.mediav.com:2191,zk13ss.prod.mediav.com:2191/kafka08 \
--group.id ${CONSUMERGROUP} \
--htablename ${OUTPUT_HTABLENAME} \
--hdfsoutdir ${OUTPUT_HDFSDIR}