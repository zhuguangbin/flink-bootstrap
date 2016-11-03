package com.mvad.flink.demo.batch

import com.mediav.data.log.LogUtils
import com.mediav.data.log.unitedlog.UnitedEvent
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job

import scala.collection.JavaConversions._

/**
  * Created by zhuguangbin on 16-9-27.
  */
object DSPCookieIndexer {
  def main(args: Array[String]) {
    // parse input arguments
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)

    if (parameterTool.getNumberOfParameters < 2) {
      System.err.println(s"Missing parameters!\nUsage: ${this.getClass.getCanonicalName} --snapshot <snapshot> " +
        s"--htablename <htablename>")
      System.exit(1)
    }

    val snapshot = parameterTool.getRequired("snapshot")
    val htablename = parameterTool.getRequired("htablename")

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val scan: Scan = new Scan
    scan.setBatch(10000)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.rootdir", "viewfs://ss-hadoop/hbase")
    conf.set("hbase.zookeeper.quorum", "nn7ss.prod.mediav.com,nn8ss.prod.mediav.com,nn9ss.prod.mediav.com")
    conf.set(TableInputFormat.INPUT_TABLE, snapshot)
    conf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))
    conf.setStrings("io.serializations", conf.get("io.serializations"), classOf[MutationSerialization].getName, classOf[ResultSerialization].getName, classOf[KeyValueSerialization].getName)

    val job = Job.getInstance(conf)
    job.setInputFormatClass(classOf[TableSnapshotInputFormat])
    TableSnapshotInputFormat.setInput(job, snapshot, new Path("viewfs://ss-hadoop/tmp/dspsessionlogbuilder"))

    val hbase = env.createHadoopInput(new TableSnapshotInputFormat, classOf[ImmutableBytesWritable], classOf[Result], job)
    val indexPuts: DataSet[(ImmutableBytesWritable,Mutation)] = hbase.flatMap(e => {
      val result = e._2
      val row = result.getRow
      val u = result.getFamilyMap(Bytes.toBytes("u")).flatMap(e => {
        val ue = LogUtils.thriftBinarydecoder(e._2, classOf[UnitedEvent])
        val mvid = ue.getLinkedIds.getMvid
        val impressionInfos = if (ue != null && ue.isSetAdvertisementInfo && (ue.getAdvertisementInfo.isSetImpressionInfos || ue.getAdvertisementInfo.isSetImpressionInfo)) {
          ue.getAdvertisementInfo.getImpressionInfos.asInstanceOf[java.util.ArrayList[com.mediav.data.log.unitedlog.ImpressionInfo]].toList
        } else {
          Seq()
        }

        val putRecords = impressionInfos.map(impressionInfo => {
          val showRequestId = impressionInfo.getShowRequestId
          val rowkey = s"${showRequestId.toString.hashCode}#${showRequestId.toString}"

          val put = new Put(Bytes.toBytes(rowkey))
          put.addColumn(Bytes.toBytes("u"), Bytes.toBytes("id"), ue.getEventTime, Bytes.toBytes(showRequestId))
          (new ImmutableBytesWritable,put)
        })
        putRecords
      })

      val s = result.getFamilyMap(Bytes.toBytes("s")).flatMap(e => {
        val ue = LogUtils.thriftBinarydecoder(e._2, classOf[UnitedEvent])
        val mvid = ue.getLinkedIds.getMvid
        val impressionInfos = if (ue != null && ue.isSetAdvertisementInfo && (ue.getAdvertisementInfo.isSetImpressionInfos || ue.getAdvertisementInfo.isSetImpressionInfo)) {
          ue.getAdvertisementInfo.getImpressionInfos.asInstanceOf[java.util.ArrayList[com.mediav.data.log.unitedlog.ImpressionInfo]].toList
        } else {
          Seq()
        }

        val putRecords = impressionInfos.map(impressionInfo => {
          val showRequestId = impressionInfo.getShowRequestId
          val rowkey = s"${showRequestId.toString.hashCode}#${showRequestId.toString}"

          val put = new Put(Bytes.toBytes(rowkey))
          put.addColumn(Bytes.toBytes("s"), Bytes.toBytes("id"), ue.getEventTime, Bytes.toBytes(showRequestId))
          (new ImmutableBytesWritable,put)
        })
        putRecords
      })

      val c = result.getFamilyMap(Bytes.toBytes("c")).map(e => {
        val ue = LogUtils.thriftBinarydecoder(e._2, classOf[UnitedEvent])
        val mvid = ue.getLinkedIds.getMvid
        val put = new Put(Bytes.toBytes(mvid))
        put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("id"), ue.getEventTime, Bytes.toBytes(ue.getAdvertisementInfo.getImpressionInfo.getShowRequestId))
        (new ImmutableBytesWritable,put)
      })
      u ++ s ++ c
    })
    val outputformat = new HadoopOutputFormat[ImmutableBytesWritable,Mutation](new TableOutputFormat[ImmutableBytesWritable](), job);
    indexPuts.output(outputformat)
    env.execute("DSPCookieIndexer")
  }
}

