#!/usr/bin/env bash
set -e
export HADOOP_CONF_DIR=/etc/hadoop/conf
export FLINK_HOME=/opt/flink-1.1.2
export FLINKRUNNER=$FLINK_HOME/bin/flink

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"
JOBJAR=`ls "$FWDIR"/lib/flink-demo*.jar`
export CLASS=com.mvad.flink.demo.batch.DSPCookieIndexer
export SNAPSHOT=dspsession_snapshot_2016-09-26
export INDEXTABLE=dspsession-idx-mvid

export QUEUE=etl
export JOBNAME=Flink:[${QUEUE}][DSPCookieIndexer]
export NUM_CONTAINERS=2000

$FLINKRUNNER run -m yarn-cluster -ynm ${JOBNAME} -yqu ${QUEUE} -yn ${NUM_CONTAINERS} -yjm 2048 -ytm 4096 \
-c ${CLASS} ${JOBJAR} \
--snapshot ${SNAPSHOT} \
--htablename ${INDEXTABLE}
