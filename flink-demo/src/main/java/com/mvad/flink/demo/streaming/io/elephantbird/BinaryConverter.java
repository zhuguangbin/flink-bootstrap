package com.mvad.flink.demo.streaming.io.elephantbird;

/**
 * A simple interface to serialize and deserialize objects
 */
public interface BinaryConverter<M> {
  /* TODO : What about exceptions?
   */

  /** Returns deserialized object. Throws if deserialization fails. */
  M fromBytes(byte[] messageBuffer) throws DecodeException;
  
  byte[] toBytes(M message);
  
}
