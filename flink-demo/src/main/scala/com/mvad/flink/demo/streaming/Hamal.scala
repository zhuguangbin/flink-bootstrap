package com.mvad.flink.demo.streaming

import java.util.{Base64, Properties}

import com.hadoop.compression.lzo.LzopCodec
import com.mvad.flink.demo.streaming.io.elephantbird.PrimitiveByteArrayWrapper
import com.mvad.flink.demo.streaming.lib.sink._
import com.mvad.flink.demo.streaming.lib.sink.bucketing.BucketingSink
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.slf4j.LoggerFactory

/**
  * Created by zhuguangbin on 16-9-12.
  */
object Hamal {

  val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    // parse input arguments
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)

    if (parameterTool.getNumberOfParameters < 6) {
      System.err.println(s"Missing parameters!\nUsage: ${this.getClass.getCanonicalName} --topic <topic> " +
        s"--bootstrap.servers <kafka brokers> --zookeeper.connect <zk quorum> --group.id <some id> " +
        s"--htablename <htablename> --hdfsoutdir <hdfsoutdir>")
      System.exit(1)
    }

    val topic = parameterTool.getRequired("topic")
    val htablename = parameterTool.getRequired("htablename")
    val hdfsoutdir = parameterTool.getRequired("hdfsoutdir")
    log.info(s"Starting Job : ${this.getClass.getName}, topic : ${topic}, hdfsoutdir : ${hdfsoutdir}, htablename : ${htablename}")

    val props = new Properties()
    props.put("bootstrap.servers", parameterTool.getRequired("bootstrap.servers"))
    props.put("zookeeper.connect", parameterTool.getRequired("zookeeper.connect"))
    props.put("group.id", parameterTool.getRequired("group.id"))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    env.enableCheckpointing(3000) // create a checkpoint every 3 secodns
    env.getConfig.setGlobalJobParameters(parameterTool) // make parameters available in the web interface
    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer09[String](topic, new SimpleStringSchema, props))

    val bucketingSink = new BucketingSink[PrimitiveByteArrayWrapper](hdfsoutdir)
    bucketingSink.setWriter(new LzoRawBlockWriter[PrimitiveByteArrayWrapper]())
    bucketingSink.setPartPrefix(topic)
    bucketingSink.setPartSuffix(LzopCodec.DEFAULT_LZO_EXTENSION)
    bucketingSink.setBucketer(new LogEventDateTimeBucketer[PrimitiveByteArrayWrapper]())
    stream.map(line => {
      val raw = Base64.getDecoder.decode(line)
      val wrapper = new PrimitiveByteArrayWrapper(raw)
      wrapper
    }).addSink(bucketingSink)


    // only put to hdfs
    /*
    val puts = stream.flatMap(line => {
      val raw = Base64.getDecoder.decode(line)
      val ue = LogUtils.ThriftBase64decoder(line, classOf[UnitedEvent])
      val eventType = ue.getEventType.getType
      val family = if (eventType == 200) "u"
      else if (eventType == 115) "s"
      else if (eventType == 99) "c"
      else throw new Exception(s"not supported eventType: ${eventType}, only support topic .u/.s/.c")

      // got all impressions
      val impressionInfos = if (ue != null && ue.isSetAdvertisementInfo && (ue.getAdvertisementInfo.isSetImpressionInfos || ue.getAdvertisementInfo.isSetImpressionInfo)) {
        if (eventType == 200 || eventType == 115) {
          ue.getAdvertisementInfo.getImpressionInfos.asInstanceOf[java.util.ArrayList[com.mediav.data.log.unitedlog.ImpressionInfo]].toList
        } else {
          Seq(ue.getAdvertisementInfo.getImpressionInfo).toList
        }
      } else {
        Seq()
      }

      // transform to put format
      val putRecords = impressionInfos.map(impressionInfo => {
        val showRequestId = impressionInfo.getShowRequestId
        val rowkey = s"${showRequestId.toString.hashCode}#${showRequestId.toString}"

        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(ue.getLogId), ue.getEventTime, raw)
        put
      })
      putRecords
    })

    val zkQuorum = "nn7ss.prod.mediav.com,nn8ss.prod.mediav.com,nn9ss.prod.mediav.com"
    puts.writeUsingOutputFormat(new HBaseOutputFormat(zkQuorum, htablename))
    */

    env.execute(this.getClass.getSimpleName + topic)

  }

}

class HBaseOutputFormat(zkQuorum: String, htablename: String) extends OutputFormat[Put] {
  private val serialVersionUID: Long = 1L

  private var table: Table = _
  private var taskNumbers: String = _
  private var rowNumbers: Int = 0


  override def configure(configuration: Configuration): Unit = {

  }

  override def close(): Unit = {
    table.close()
  }

  override def writeRecord(put: Put): Unit = {
    rowNumbers += 1
    table.put(put)
  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkQuorum)
    val conn: Connection = ConnectionFactory.createConnection(conf)
    table = conn.getTable(TableName.valueOf(htablename))
    taskNumbers = taskNumber.toString
  }
}
