package com.mvad.flink.demo.streaming

import java.util.Properties
import java.util.concurrent.TimeUnit.SECONDS

import com.mediav.data.log.unitedlog.UnitedEvent
import com.twitter.chill.thrift.TBaseSerializer
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.LoggerFactory
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
    env.registerTypeWithKryoSerializer(classOf[UnitedEvent], classOf[TBaseSerializer])
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.getConfig.disableSysoutLogging
    env.enableCheckpointing(3000) // create a checkpoint every 3 seconds
    env.getConfig.setGlobalJobParameters(parameterTool) // make parameters available in the web interface

    val u: DataStream[UnitedEvent] = env.addSource(new FlinkKafkaConsumer[UnitedEvent](topic, new UnitedEventSchema, props))
    // flat impression
    val count = u.filter(ue => (ue.getEventType.getType == 200) && ue.isSetContextInfo && ue.getContextInfo.isSetGeoInfo).map(ue => ((ue.getContextInfo.getGeoInfo.getCountry, ue.getContextInfo.getGeoInfo.getProvince), 1))
      .keyBy(0).timeWindow(Time.of(5, SECONDS)).reduce(((v1, v2) => (v1._1, v1._2 + v2._2)))
    count.print()

    env.execute("DSPUserArealDistribution")

  }

}
