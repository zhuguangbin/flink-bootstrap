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
object Hamal2HDFS {

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

    env.execute(this.getClass.getSimpleName + topic)

  }

}
