package com.mvad.flink.demo.streaming.io.elephantbird;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A {@link BinaryLineWriter} where each record is a byte array.
 */
public class BinaryLineWriter {
  private final OutputStream out_;

  public BinaryLineWriter(OutputStream out) {
    out_ = out;
  }

  public void write(byte[] data) throws IOException {
    out_.write(data);
    out_.write('\n');
  }

  public void close() throws IOException {
    out_.close();
  }
}
