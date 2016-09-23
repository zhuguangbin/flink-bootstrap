package com.mvad.flink.demo.streaming.io.elephantbird;

/**
 * Created by zhuguangbin on 16-9-14.
 */
public class PrimitiveByteArrayWrapper {

  private byte[] byteArray = null;

  public PrimitiveByteArrayWrapper(byte[] byteArray) {
    this.byteArray = byteArray;
  }

  public byte[] getByteArray() {
    return byteArray;
  }

  public void setByteArray(byte[] byteArray) {
    this.byteArray = byteArray;
  }
}
