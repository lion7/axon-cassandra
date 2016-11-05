package org.axonframework.cassandra.eventsourcing.eventstore;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Transient;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.DomainEventData;
import org.axonframework.serialization.*;

import java.nio.ByteBuffer;
import java.time.Instant;

@Table(name = "SnapshotEventEntry", caseSensitiveTable = true)
public class SnapshotEventEntry implements DomainEventData<byte[]> {

    @Column(caseSensitive = true)
    private String eventIdentifier;
    @Column(caseSensitive = true)
    private String timeStamp;
    @Column(caseSensitive = true)
    private String payloadType;
    @Column(caseSensitive = true)
    private String payloadRevision;
    @Column(caseSensitive = true)
    private ByteBuffer payloadBuffer;
    @Column(caseSensitive = true)
    private ByteBuffer metaDataBuffer;
    @Column(caseSensitive = true)
    private String type;
    @Column(caseSensitive = true)
    @PartitionKey
    private String aggregateIdentifier;
    @Column(caseSensitive = true)
    private long sequenceNumber;

    public SnapshotEventEntry(DomainEventMessage<?> eventMessage, Serializer serializer) {
        SerializedObject<byte[]> payload = serializer.serialize(eventMessage.getPayload(), byte[].class);
        SerializedObject<byte[]> metaData = serializer.serialize(eventMessage.getMetaData(), byte[].class);
        this.eventIdentifier = eventMessage.getIdentifier();
        this.payloadType = payload.getType().getName();
        this.payloadRevision = payload.getType().getRevision();
        this.payloadBuffer = ByteBuffer.wrap(payload.getData());
        this.metaDataBuffer = ByteBuffer.wrap(metaData.getData());
        this.timeStamp = eventMessage.getTimestamp().toString();
        this.type = eventMessage.getType();
        this.aggregateIdentifier = eventMessage.getAggregateIdentifier();
        this.sequenceNumber = eventMessage.getSequenceNumber();
    }

    protected SnapshotEventEntry() {
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public String getAggregateIdentifier() {
        return aggregateIdentifier;
    }

    @Override
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public String getEventIdentifier() {
        return eventIdentifier;
    }

    @Override
    @Transient
    public Instant getTimestamp() {
        return Instant.parse(timeStamp);
    }

    @Override
    @Transient
    @SuppressWarnings("unchecked")
    public SerializedObject<byte[]> getMetaData() {
        return new SerializedMetaData<>(metaDataBuffer.array(), byte[].class);
    }

    @Override
    @Transient
    @SuppressWarnings("unchecked")
    public SerializedObject<byte[]> getPayload() {
        return new SimpleSerializedObject<>(payloadBuffer.array(), byte[].class, getPayloadType());
    }

    protected SerializedType getPayloadType() {
        return new SimpleSerializedType(payloadType, payloadRevision);
    }
}
