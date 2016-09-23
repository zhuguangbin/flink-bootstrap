package com.mvad.flink.demo.streaming.lib.sink;

import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzopCodec;
import com.mediav.data.log.LogUtils;
import com.mediav.data.log.unitedlog.ImpressionInfo;
import com.mediav.data.log.unitedlog.UnitedEvent;
import com.mvad.flink.demo.streaming.io.elephantbird.PrimitiveByteArrayWrapper;
import com.mvad.flink.demo.streaming.io.elephantbird.RawBlockWriter;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.streaming.connectors.fs.StreamWriterBase;
import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhuguangbin on 16-9-14.
 */
public class RawlogDualWriter<T extends PrimitiveByteArrayWrapper> extends StreamWriterBase<T> {
  private static Logger LOG = LoggerFactory.getLogger(RawlogDualWriter.class);

  private static final long indexBlockSize = 33554432L;

  private transient FileSystem fs = null;
  private transient FSDataOutputStream outStream;
  private transient Path file = null;
  private transient Path tmpIndexFile = null;
  private transient Path finalIndexFile = null;
  private transient RawBlockWriter writer = null;
  private transient Table table = null;
  private String htablename = null;

  public RawlogDualWriter(String htablename) {
    this.htablename = htablename;
  }

  @Override
  public void open(FileSystem fs, Path path) throws IOException {
    this.fs = fs;
    this.file = path;

    if (this.outStream != null) {
      throw new IllegalStateException("Writer has already been opened");
    } else {
      if (fs.exists(file)) {
        fs.delete(file, false);
      }
      this.outStream = fs.create(file, false);
    }

    this.tmpIndexFile = file.suffix(LzoIndex.LZO_TMP_INDEX_SUFFIX);
    this.finalIndexFile = new Path(file.getParent(), file.getName().replace("_", "").replace(".in-progress", "")).suffix(LzoIndex.LZO_INDEX_SUFFIX);
    if (fs.exists(this.tmpIndexFile)) {
      fs.delete(this.tmpIndexFile, false);
    }
    FSDataOutputStream indexOut = fs.create(tmpIndexFile, false);

    Configuration conf = HadoopFileSystem.getHadoopConfiguration();
    LzopCodec codec = new LzopCodec();
    codec.setConf(conf);
    CompressionOutputStream compressionOutputStream = codec.createIndexedOutputStream(outStream, indexOut);
    this.writer = new RawBlockWriter(compressionOutputStream);

    Configuration hbaseConf = HBaseConfiguration.create(conf);
    hbaseConf.set("hbase.zookeeper.quorum", "nn7ss.prod.mediav.com,nn8ss.prod.mediav.com,nn9ss.prod.mediav.com");
    Connection conn = ConnectionFactory.createConnection(hbaseConf);
    this.table = conn.getTable(TableName.valueOf(htablename));
  }

  @Override
  public void write(T t) throws IOException {
    byte[] raw = t.getByteArray();
    this.writer.write(raw);
    try {
      UnitedEvent ue = LogUtils.thriftBinarydecoder(raw, UnitedEvent.class);
      int eventType = ue.getEventType().getType();
      String family = null;
      if (eventType == 200) {
        family = "u";
      } else if (eventType == 115) {
        family = "s";
      } else if (eventType == 99) {
        family = "c";
      }

      if (ue != null && ue.isSetAdvertisementInfo() && (ue.getAdvertisementInfo().isSetImpressionInfos() || ue.getAdvertisementInfo().isSetImpressionInfo())) {

        if (eventType == 200 || eventType == 115) {
          // .u or .s
          List<ImpressionInfo> impressionInfos = ue.getAdvertisementInfo().getImpressionInfos();
          for (ImpressionInfo impression : impressionInfos) {
            Long showRequestId = impression.getShowRequestId();
            String rowkey = showRequestId.toString().hashCode() + "#" + showRequestId.toString();
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(ue.getLogId()), ue.getEventTime(), raw);
            this.table.put(put);
          }
        } else if (eventType == 99) {
          // .c
          ImpressionInfo impression = ue.getAdvertisementInfo().getImpressionInfo();
          Long showRequestId = impression.getShowRequestId();
          String rowkey = showRequestId.toString().hashCode() + "#" + showRequestId.toString();
          Put put = new Put(Bytes.toBytes(rowkey));
          put.addColumn(Bytes.toBytes(family), Bytes.toBytes(ue.getLogId()), ue.getEventTime(), raw);
          this.table.put(put);
        }
      }
    } catch (Exception e) {
      LOG.error("error writing hbase :", e);
      e.printStackTrace();
    }
  }

  @Override
  public long flush() throws IOException {
    if (this.outStream == null) {
      throw new IllegalStateException("Writer is not open");
    } else {
      this.outStream.hflush();
      return this.outStream.getPos();
    }
  }

  @Override
  public long getPos() throws IOException {
    if (this.outStream == null) {
      throw new IllegalStateException("Writer is not open");
    } else {
      return this.outStream.getPos();
    }
  }

  @Override
  public void close() throws IOException {
    if (this.writer != null) {
      this.writer.close();
      this.outStream = null;
    }
    if (this.table != null) {
      this.table.close();
    }

    // rename or remove the index file based on file size.
    FileStatus stat = fs.getFileStatus(file);
    if (stat.getLen() <= indexBlockSize) {
      fs.delete(tmpIndexFile, false);
    } else {
      fs.rename(tmpIndexFile, finalIndexFile);
    }
  }

  @Override
  public Writer<T> duplicate() {
    return new RawlogDualWriter(htablename);
  }
}
