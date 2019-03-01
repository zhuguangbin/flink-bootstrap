package com.mvad.flink.demo.streaming

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.mediav.data.log.unitedlog.UnitedEvent
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.TBinaryProtocol
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by zhuguangbin on 16-9-12.
  */

case class Session(uid: Long, sid: Long)

object RealTimeSession {

  val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    // parse input arguments
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)

    if (parameterTool.getNumberOfParameters < 4) {
      System.err.println(s"Missing parameters!\nUsage: ${this.getClass.getCanonicalName} --topic <topic> " +
        s"--bootstrap.servers <kafka brokers> --group.id <some id> " +
        s"--hdfsoutdir <hdfsoutdir>")
      System.exit(1)
    }

    val topic = parameterTool.getRequired("topic")
    val hdfsoutdir = parameterTool.getRequired("hdfsoutdir")
    log.info(s"Starting Job : ${this.getClass.getName}, topic : ${topic}, hdfsoutdir : ${hdfsoutdir}")

    val props = new Properties()
    props.put("bootstrap.servers", parameterTool.getRequired("bootstrap.servers"))
    props.put("group.id", parameterTool.getRequired("group.id"))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging
    env.enableCheckpointing(3000) // create a checkpoint every 3 secodns
    env.getConfig.setGlobalJobParameters(parameterTool) // make parameters available in the web interface

    val s: DataStream[UnitedEvent] = env.addSource(new FlinkKafkaConsumer[UnitedEvent]("d.s.6", new UnitedEventSchema, props))
    s.filter(ue => ue.getEventType.getType == 115).flatMap(ue => {
      // got all impressions
      val impressionInfos = if (ue != null && ue.isSetAdvertisementInfo && (ue.getAdvertisementInfo.isSetImpressionInfos || ue.getAdvertisementInfo.isSetImpressionInfo)) {
        ue.getAdvertisementInfo.getImpressionInfos.asInstanceOf[java.util.ArrayList[com.mediav.data.log.unitedlog.ImpressionInfo]].toList
      } else {
        Seq()
      }
      impressionInfos.map(impressionInfo => (ue.getLogId, ue.getEventTime, new TSerializer(new TBinaryProtocol.Factory()).serialize(ue)))
    }).map(log => {
      val put = new Put(Bytes.toBytes(log._1))
      put.addColumn(Bytes.toBytes("s"), Bytes.toBytes(log._2), log._3)
      put
    }).writeUsingOutputFormat(new HBaseOutputFormat("nn7ss.prod.mediav.com,nn8ss.prod.mediav.com,nn9ss.prod.mediav.com", "session"))


    val u: DataStream[UnitedEvent] = env.addSource(new FlinkKafkaConsumer[UnitedEvent]("d.u.6", new UnitedEventSchema, props))
    // flat impression
    val uImpression = u.filter(ue => ue.getEventType.getType == 200).flatMap(ue => {
      // got all impressions
      val impressionInfos = if (ue != null && ue.isSetAdvertisementInfo && (ue.getAdvertisementInfo.isSetImpressionInfos || ue.getAdvertisementInfo.isSetImpressionInfo)) {
        ue.getAdvertisementInfo.getImpressionInfos.asInstanceOf[java.util.ArrayList[com.mediav.data.log.unitedlog.ImpressionInfo]].toList
      } else {
        Seq()
      }
      impressionInfos.map(impressionInfo => (ue.getLogId, ue.getEventTime, new TSerializer(new TBinaryProtocol.Factory()).serialize(ue)))
    })

    val su = uImpression.map(_.toString())

    // u iterate to join s
    val out = su.iterate(
      iteration => {
        val iterationBody = AsyncDataStream.unorderedWait(iteration, new AsyncHBaseGet(), 1000, TimeUnit.MILLISECONDS, 100)
        val feedback = iterationBody.filter(u => u.length > 0)
        val output = iterationBody.filter(u => u.length < 0).map(u => u.length)
        (feedback, output)
      }
    )

    // joined us output back to kafka
    //    out.addSink(new FlinkKafkaProducer[(Long, Long, Long)]())

    env.execute("realTimeSession")

  }

  class AsyncHBaseGet extends AsyncFunction[String, String] {

    /** The database specific client that can issue concurrent requests with callbacks */
    //    lazy val client: DatabaseClient = new DatabaseClient(host, post, credentials)

    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.hbase.HBaseConfiguration
    import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

    val conf: Configuration = HBaseConfiguration.create
    val connection: Connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf("session"))

    /** The context used for the future callbacks */
    implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())


    override def asyncInvoke(key: String, resultFuture: ResultFuture[String]): Unit = {


      // issue the asynchronous request, receive a future for the result
      val resultFutureRequested: Future[String] = Future {
        val result = table.get(new Get(Bytes.toBytes(key)))
        result.toString
      }

      // set the callback to be executed once the request by the client is complete
      // the callback simply forwards the result to the result future
      resultFutureRequested.onSuccess {
        case result: String => resultFuture.complete(Iterable(result))
      }
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

}
