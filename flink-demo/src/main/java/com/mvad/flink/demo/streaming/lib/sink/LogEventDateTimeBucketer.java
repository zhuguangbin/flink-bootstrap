package com.mvad.flink.demo.streaming.lib.sink;

import com.mediav.data.log.unitedlog.ContextInfo;
import com.mediav.data.log.unitedlog.UnitedEvent;
import com.mvad.flink.demo.streaming.io.elephantbird.PrimitiveByteArrayWrapper;
import com.mvad.flink.demo.streaming.lib.sink.bucketing.Bucketer;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogEventDateTimeBucketer<T extends PrimitiveByteArrayWrapper> implements Bucketer<T> {
  private static final long serialVersionUID = 1L;

  private static Logger LOG = LoggerFactory.getLogger(LogEventDateTimeBucketer.class);

  private static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd/HH";

  private final String formatString;

  private transient SimpleDateFormat dateFormatter;
  private transient TDeserializer deserializer;

  public LogEventDateTimeBucketer() {
    this(DEFAULT_FORMAT_STRING);
  }

  public LogEventDateTimeBucketer(String formatString) {
    this.formatString = formatString;
    this.dateFormatter = new SimpleDateFormat(formatString);
    this.deserializer = new TDeserializer(new TBinaryProtocol.Factory());
  }

  @Override
  public Path getBucketPath(Clock clock, Path basePath, T element) {
    long eventTimeInMs = 0;
    String device = "UNKNOWN";
    try {
      eventTimeInMs = deserializer.partialDeserializeI64(element.getByteArray(), UnitedEvent._Fields.EVENT_TIME);
      final ContextInfo contextInfo = new ContextInfo();
      deserializer.partialDeserialize(contextInfo, element.getByteArray(), UnitedEvent._Fields.CONTEXT_INFO);
      device = contextInfo.isSetMobileInfo() ? "mobile" : "pc";
    } catch (TException e) {
      LOG.warn("error UE, exception when deserializing, ", e);
    }
    String newDateTimeString = dateFormatter.format(new Date(eventTimeInMs));
    return new Path(basePath + "/" + newDateTimeString + "/" + device);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();

    this.dateFormatter = new SimpleDateFormat(formatString);
    this.deserializer = new TDeserializer(new TBinaryProtocol.Factory());
  }

  @Override
  public String toString() {
    return "LogEventDateTimeBucketer{" +
            "deserializer='" + deserializer.toString() + '\'' + ',' +
            "formatString='" + dateFormatter.toPattern() + '\'' +
            '}';
  }
}
