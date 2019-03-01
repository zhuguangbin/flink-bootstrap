package com.mvad.flink.demo.streaming

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.mediav.data.log.unitedlog.UnitedEvent
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
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


object DSPUserArealDistribution {

  val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    // parse input arguments
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)

    if (parameterTool.getNumberOfParameters < 3) {
      System.err.println(s"Missing parameters!\nUsage: ${this.getClass.getCanonicalName} --topic <topic> " +
        s"--bootstrap.servers <kafka brokers> --group.id <some id> ")
      System.exit(1)
    }

    val topic = parameterTool.getRequired("topic")
    val brokers = parameterTool.getRequired("bootstrap.servers")
    val groupId = parameterTool.getRequired("group.id")
    log.info(s"Starting Job : ${this.getClass.getName}, brokers: ${brokers}, topic : ${topic}, group.id: ${groupId}")

    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("group.id", groupId)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.getConfig.disableSysoutLogging
    env.enableCheckpointing(3000) // create a checkpoint every 3 seconds
    env.getConfig.setGlobalJobParameters(parameterTool) // make parameters available in the web interface

    val u: DataStream[UnitedEvent] = env.addSource(new FlinkKafkaConsumer[UnitedEvent](topic, new UnitedEventSchema, props))
    // flat impression
    val uImpression = u.filter(ue => (ue.getEventType.getType == 200) && ue.isSetContextInfo && ue.getContextInfo.isSetGeoInfo).map(ue => ((ue.getContextInfo.getGeoInfo.getCountry, ue.getContextInfo.getGeoInfo.getProvince), 1))
      .keyBy(_._1).window(TumblingEventTimeWindows.of(Time.minutes(1))).reduce((v1, v2) => (v1._1, v1._2 + v2._2)).print()


    //    val u: DataStream[UnitedEvent] = env.addSource(new FlinkKafkaConsumer[UnitedEvent]("d.u.6", new UnitedEventSchema, props))
    //    // flat impression
    //    val uImpression = u.filter(ue => ue.getEventType.getType == 200).flatMap(ue => {
    //      // got all impressions
    //      val impressionInfos = if (ue != null && ue.isSetAdvertisementInfo && (ue.getAdvertisementInfo.isSetImpressionInfos || ue.getAdvertisementInfo.isSetImpressionInfo)) {
    //        ue.getAdvertisementInfo.getImpressionInfos.asInstanceOf[java.util.ArrayList[com.mediav.data.log.unitedlog.ImpressionInfo]].toList
    //      } else {
    //        Seq()
    //      }
    //      impressionInfos.map(impressionInfo => (ue.getLogId, ue.getEventTime, new TSerializer(new TBinaryProtocol.Factory()).serialize(ue)))
    //    })
    //
    //    val su = uImpression.map(_.toString())
    //
    //    // u iterate to join s
    //    val out = su.iterate(
    //      iteration => {
    //        val iterationBody = AsyncDataStream.unorderedWait(iteration, new AsyncHBaseGet(), 1000, TimeUnit.MILLISECONDS, 100)
    //        val feedback = iterationBody.filter(u => u.length > 0)
    //        val output = iterationBody.filter(u => u.length < 0).map(u => u.length)
    //        (feedback, output)
    //      }
    //    )

    // joined us output back to kafka
    //    out.addSink(new FlinkKafkaProducer[(Long, Long, Long)]())

    env.execute("realTimeSession")

  }

}
