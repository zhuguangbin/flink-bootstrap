package com.mvad.flink.demo.streaming

import java.util.{Base64, Properties}

import com.mediav.data.log.LogUtils
import com.mediav.data.log.unitedlog.UnitedEvent
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
  * Created by zhuguangbin on 16-9-12.
  */
object Hamal2HBase {

  val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    // parse input arguments
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)

    if (parameterTool.getNumberOfParameters < 4) {
      System.err.println(s"Missing parameters!\nUsage: ${this.getClass.getCanonicalName} --topic <topic> " +
        s"--bootstrap.servers <kafka brokers> --group.id <some id> " +
        s"--htablename <htablename>")
      System.exit(1)
    }

    val topic = parameterTool.getRequired("topic")
    val htablename = parameterTool.getRequired("htablename")
    log.info(s"Starting Job : ${this.getClass.getName}, topic : ${topic}, htablename : ${htablename}")

    val props = new Properties()
    props.put("bootstrap.servers", parameterTool.getRequired("bootstrap.servers"))
    props.put("group.id", parameterTool.getRequired("group.id"))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging
    env.enableCheckpointing(3000) // create a checkpoint every 3 secodns
    env.getConfig.setGlobalJobParameters(parameterTool) // make parameters available in the web interface
    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](topic, new SimpleStringSchema, props))

    val puts = stream.flatMap(line => {
      val raw = Base64.getDecoder.decode(line)
      val ue = LogUtils.thriftBinarydecoder(raw, classOf[UnitedEvent])
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

    env.execute(this.getClass.getSimpleName + topic)
  }

}

class HBaseOutputFormat(zkQuorum: String, htablename: String) extends OutputFormat[Put] {
  private val serialVersionUID: Long = 1L

  private var table: HTable = _
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
    table = new HTable(conf, htablename)
    table.setWriteBufferSize(6 * 1024 * 1024)
    table.setAutoFlush(false)
    taskNumbers = taskNumber.toString
  }
}
