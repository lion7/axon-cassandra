package org.axonframework.cassandra.eventsourcing.eventstore;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Transient;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.DomainEventData;
import org.axonframework.eventsourcing.eventstore.GlobalSequenceTrackingToken;
import org.axonframework.eventsourcing.eventstore.TrackedEventData;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.serialization.*;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Date;

@Table(name = "DomainEventEntry", caseSensitiveTable = true)
public class DomainEventEntry implements DomainEventData<byte[]>, TrackedEventData<byte[]> {

    @Column(caseSensitive = true)
    private String eventIdentifier;
    @Column(caseSensitive = true)
    private Date timeStamp;
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
    @PartitionKey(1)
    private long sequenceNumber;
    @Column(caseSensitive = true)
    private long globalIndex;

    public DomainEventEntry(long globalIndex, DomainEventMessage<?> eventMessage, Serializer serializer) {
        SerializedObject<byte[]> payload = serializer.serialize(eventMessage.getPayload(), byte[].class);
        SerializedObject<byte[]> metaData = serializer.serialize(eventMessage.getMetaData(), byte[].class);
        this.eventIdentifier = eventMessage.getIdentifier();
        this.payloadType = payload.getType().getName();
        this.payloadRevision = payload.getType().getRevision();
        this.payloadBuffer = ByteBuffer.wrap(payload.getData());
        this.metaDataBuffer = ByteBuffer.wrap(metaData.getData());
        this.timeStamp = new Date(eventMessage.getTimestamp().toEpochMilli());
        this.type = eventMessage.getType();
        this.aggregateIdentifier = eventMessage.getAggregateIdentifier();
        this.sequenceNumber = eventMessage.getSequenceNumber();
        this.globalIndex = globalIndex;
    }

    protected DomainEventEntry() {
    }

    public long getGlobalIndex() {
        return globalIndex;
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
    public TrackingToken trackingToken() {
        return new GlobalSequenceTrackingToken(globalIndex);
    }

    @Override
    public String getEventIdentifier() {
        return eventIdentifier;
    }

    @Override
    @Transient
    public Instant getTimestamp() {
        return timeStamp.toInstant();
    }

    @Override
    @Transient
    @SuppressWarnings("unchecked")
    public SerializedObject<byte[]> getMetaData() {
        return new SerializedMetaData<>(metaDataBuffer != null ? metaDataBuffer.array() : null, byte[].class);
    }

    @Override
    @Transient
    @SuppressWarnings("unchecked")
    public SerializedObject<byte[]> getPayload() {
        return new SimpleSerializedObject<>(payloadBuffer != null ? payloadBuffer.array() : null, byte[].class, getPayloadType());
    }

    protected SerializedType getPayloadType() {
        return new SimpleSerializedType(payloadType, payloadRevision);
    }
}
