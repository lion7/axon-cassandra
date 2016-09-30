package org.axonframework.cassandra.eventsourcing.eventstore;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Created by gle21221 on 10-9-2016.
 */
@Table(name = "EventLogEntry", caseSensitiveTable = true)
public class EventLogEntry implements Comparable<EventLogEntry> {
    public static final int BATCH_SIZE = 100;
    @Column(caseSensitive = true)
    @PartitionKey
    private final long batchIndex;
    @Column(caseSensitive = true)
    @PartitionKey(1)
    private final long globalIndex;
    @Column(caseSensitive = true)
    private final String aggregateIdentifier;
    @Column(caseSensitive = true)
    private final long sequenceNumber;

    public EventLogEntry(long globalIndex, String aggregateIdentifier, long sequenceNumber) {
        this.batchIndex = determineBatchIndex(globalIndex);
        this.globalIndex = globalIndex;
        this.aggregateIdentifier = aggregateIdentifier;
        this.sequenceNumber = sequenceNumber;
    }

    public static long determineBatchIndex(long globalIndex) {
        return globalIndex % BATCH_SIZE;
    }

    public long getBatchIndex() {
        return batchIndex;
    }

    public long getGlobalIndex() {
        return globalIndex;
    }

    public String getAggregateIdentifier() {
        return aggregateIdentifier;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EventLogEntry that = (EventLogEntry) o;

        return globalIndex == that.globalIndex && aggregateIdentifier.equals(that.aggregateIdentifier) && sequenceNumber == that.sequenceNumber;

    }

    @Override
    public int hashCode() {
        int result = (int) (globalIndex ^ (globalIndex >>> 32));
        result = 31 * result + aggregateIdentifier.hashCode();
        result = 31 * result + (int) (sequenceNumber ^ (sequenceNumber >>> 32));
        return result;
    }

    @Override
    public int compareTo(EventLogEntry that) {
        int result = Long.compare(this.globalIndex, that.globalIndex);
        if (result == 0) {
            result = this.aggregateIdentifier.compareTo(that.aggregateIdentifier);
        }
        if (result == 0) {
            result = Long.compare(this.sequenceNumber, that.sequenceNumber);
        }
        return 0;
    }

    @Override
    public String toString() {
        return "EventLogEntry{" +
                "globalIndex=" + globalIndex +
                ", aggregateIdentifier='" + aggregateIdentifier + '\'' +
                ", sequenceNumber=" + sequenceNumber +
                '}';
    }
}
