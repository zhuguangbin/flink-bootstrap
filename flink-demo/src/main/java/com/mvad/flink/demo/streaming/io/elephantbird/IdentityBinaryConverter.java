package com.mvad.flink.demo.streaming.io.elephantbird;

/**
 * A noop {@link BinaryConverter} that returns the input bytes unmodified.
 */
public class IdentityBinaryConverter implements BinaryConverter<byte[]> {

  @Override
  public byte[] fromBytes(byte[] messageBuffer) {
    return messageBuffer;
  }

  @Override
  public byte[] toBytes(byte[] message) {
    return message;
  }

}
