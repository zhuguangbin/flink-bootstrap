package com.mvad.flink.demo.streaming;

import com.mediav.data.log.unitedlog.UnitedEvent;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.IOException;

public class UnitedEventSchema extends AbstractDeserializationSchema<UnitedEvent> {

    private static final ThreadLocal<TDeserializer> thriftDeserializer = new ThreadLocal<TDeserializer>() {
        @Override
        protected TDeserializer initialValue() {
            return new TDeserializer(new TBinaryProtocol.Factory());
        }
    };


    @Override
    public UnitedEvent deserialize(byte[] bytes) throws IOException {
        UnitedEvent ue = new UnitedEvent();
        try {
            thriftDeserializer.get().deserialize(ue, bytes);
        } catch (TException e) {
            throw new IOException(e);
        }
        return ue;
    }
}
