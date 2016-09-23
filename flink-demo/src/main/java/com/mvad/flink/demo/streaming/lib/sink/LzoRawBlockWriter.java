package com.mvad.flink.demo.streaming.lib.sink;

import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzopCodec;
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
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by zhuguangbin on 16-9-14.
 */
public class LzoRawBlockWriter<T extends PrimitiveByteArrayWrapper> extends StreamWriterBase<T> {
  private static Logger LOG = LoggerFactory.getLogger(LzoRawBlockWriter.class);

  private static final long indexBlockSize = 33554432L;

  private transient FileSystem fs = null;
  private transient FSDataOutputStream outStream;
  private transient Path file = null;
  private transient Path tmpIndexFile = null;
  private transient Path finalIndexFile = null;
  private transient RawBlockWriter writer = null;

  @Override
  public void open(FileSystem fs, Path path) throws IOException {
    this.fs = fs;
    this.file = path;

    if(this.outStream != null) {
      throw new IllegalStateException("Writer has already been opened");
    } else {
      if (fs.exists(file)){
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
    writer = new RawBlockWriter(compressionOutputStream);
  }

  @Override
  public void write(T t) throws IOException {
    writer.write(t.getByteArray());
  }

  @Override
  public long flush() throws IOException {
    if(this.outStream == null) {
      throw new IllegalStateException("Writer is not open");
    } else {
      this.outStream.hflush();
      return this.outStream.getPos();
    }
  }

  @Override
  public long getPos() throws IOException {
    if(this.outStream == null) {
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
    return new LzoRawBlockWriter();
  }
}
