package org.axonframework.cassandra.eventsourcing.eventstore;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;
import org.axonframework.serialization.SimpleSerializedType;

import java.nio.ByteBuffer;
import java.util.Date;

@Table(name = "TokenEntry", caseSensitiveTable = true)
public class TokenEntry {
    @Column(caseSensitive = true)
    @PartitionKey
    private String processorName;
    @Column(caseSensitive = true)
    @PartitionKey(1)
    private int segment;
    @Column(caseSensitive = true)
    private ByteBuffer tokenBuffer;
    @Column(caseSensitive = true)
    private String tokenType;
    @Column(caseSensitive = true)
    private Date timeStamp;

    public TokenEntry(String process, int segment, TrackingToken tokenBuffer, Serializer serializer) {
        SerializedObject<byte[]> serializedToken = serializer.serialize(tokenBuffer, byte[].class);
        this.processorName = process;
        this.segment = segment;
        this.tokenBuffer = ByteBuffer.wrap(serializedToken.getData());
        this.tokenType = serializedToken.getType().getName();
        this.timeStamp = new Date();
    }

    protected TokenEntry() {
    }

    public TrackingToken trackingToken(Serializer serializer) {
        SimpleSerializedObject<byte[]> serializedObject =
                new SimpleSerializedObject<>(tokenBuffer != null ? tokenBuffer.array() : null, byte[].class, new SimpleSerializedType(tokenType, null));
        return serializer.deserialize(serializedObject);
    }
}
