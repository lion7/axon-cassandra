package org.axonframework.cassandra.eventsourcing.eventstore;

import com.datastax.driver.core.Session;

import java.util.Arrays;

public class EventSchema {

    private final String domainEventTable, snapshotTable, globalIndexColumn, timestampColumn, eventIdentifierColumn,
            aggregateIdentifierColumn, sequenceNumberColumn, typeColumn, payloadTypeColumn, payloadRevisionColumn,
            payloadColumn, metaDataColumn, countersTable, eventLogTable, batchIndexColumn, nameColumn, valueColumn;

    public EventSchema() {
        this(builder());
    }

    protected EventSchema(Builder builder) {
        domainEventTable = builder.domainEventTable;
        snapshotTable = builder.snapshotTable;
        globalIndexColumn = builder.globalIndexColumn;
        timestampColumn = builder.timestampColumn;
        eventIdentifierColumn = builder.eventIdentifierColumn;
        aggregateIdentifierColumn = builder.aggregateIdentifierColumn;
        sequenceNumberColumn = builder.sequenceNumberColumn;
        typeColumn = builder.typeColumn;
        payloadTypeColumn = builder.payloadTypeColumn;
        payloadRevisionColumn = builder.payloadRevisionColumn;
        payloadColumn = builder.payloadColumn;
        metaDataColumn = builder.metaDataColumn;
        countersTable = builder.countersTable;
        eventLogTable = builder.eventLogTable;
        batchIndexColumn = builder.batchIndexColumn;
        nameColumn = builder.nameColumn;
        valueColumn = builder.valueColumn;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String domainEventTable() {
        return domainEventTable;
    }

    public String snapshotTable() {
        return snapshotTable;
    }

    public String globalIndexColumn() {
        return globalIndexColumn;
    }

    public String timestampColumn() {
        return timestampColumn;
    }

    public String eventIdentifierColumn() {
        return eventIdentifierColumn;
    }

    public String aggregateIdentifierColumn() {
        return aggregateIdentifierColumn;
    }

    public String sequenceNumberColumn() {
        return sequenceNumberColumn;
    }

    public String typeColumn() {
        return typeColumn;
    }

    public String payloadTypeColumn() {
        return payloadTypeColumn;
    }

    public String payloadRevisionColumn() {
        return payloadRevisionColumn;
    }

    public String payloadColumn() {
        return payloadColumn;
    }

    public String metaDataColumn() {
        return metaDataColumn;
    }

    public String countersTable() {
        return countersTable;
    }

    public String eventLogTable() {
        return eventLogTable;
    }

    public String batchIndexColumn() {
        return batchIndexColumn;
    }

    public String nameColumn() {
        return nameColumn;
    }

    public String valueColumn() {
        return valueColumn;
    }

    public void initialize(Session session) {
        session.execute(initializeDomainEventTable());
        session.execute(initializeSnapshotEventTable());
        session.execute(initializeEventLogTable());
        session.execute(initializeCountersTable());
    }

    private String initializeDomainEventTable() {
        return "CREATE TABLE IF NOT EXISTS " + quoted(domainEventTable) + " (" +
                quoted(aggregateIdentifierColumn) + " varchar, " +
                quoted(sequenceNumberColumn) + " bigint, " +
                quoted(typeColumn) + " varchar, " +
                quoted(eventIdentifierColumn) + " varchar, " +
                quoted(metaDataColumn) + " blob, " +
                quoted(payloadColumn) + " blob, " +
                quoted(payloadRevisionColumn) + " varchar, " +
                quoted(payloadTypeColumn) + " varchar, " +
                quoted(timestampColumn) + " timestamp, " +
                quoted(globalIndexColumn) + " bigint, " +
                "PRIMARY KEY(" + quoted(aggregateIdentifierColumn) + ", " + quoted(sequenceNumberColumn) + ")" +
                ");";
    }

    private String initializeSnapshotEventTable() {
        return "CREATE TABLE IF NOT EXISTS " + quoted(snapshotTable) + " (" +
                quoted(aggregateIdentifierColumn) + " varchar, " +
                quoted(sequenceNumberColumn) + " bigint, " +
                quoted(typeColumn) + " varchar, " +
                quoted(eventIdentifierColumn) + " varchar, " +
                quoted(metaDataColumn) + " blob, " +
                quoted(payloadColumn) + " blob, " +
                quoted(payloadRevisionColumn) + " varchar, " +
                quoted(payloadTypeColumn) + " varchar, " +
                quoted(timestampColumn) + " timestamp, " +
                "PRIMARY KEY(" + quoted(aggregateIdentifierColumn) + ")" +
                ");";
    }

    private String initializeEventLogTable() {
        return "CREATE TABLE IF NOT EXISTS " + quoted(eventLogTable) + " (" +
                quoted(batchIndexColumn) + " bigint, " +
                quoted(globalIndexColumn) + " bigint, " +
                quoted(aggregateIdentifierColumn) + " varchar, " +
                quoted(sequenceNumberColumn) + " bigint, " +
                "PRIMARY KEY(" + quoted(batchIndexColumn) + ", " + quoted(globalIndexColumn) + ")" +
                ");";
    }

    private String initializeCountersTable() {
        return "CREATE TABLE IF NOT EXISTS " + quoted(countersTable) + " (" +
                quoted(nameColumn) + " varchar, " +
                quoted(valueColumn) + " bigint, " +
                "PRIMARY KEY(" + quoted(nameColumn) + ")" +
                ");";
    }

    static String quoted(String... input) {
        return quoted(Arrays.asList(input));
    }

    static String quoted(Iterable<String> input) {
        return '"' + String.join("\", \"", input) + '"';
    }

    public static class Builder {
        private String domainEventTable = "DomainEventEntry";
        private String snapshotTable = "SnapshotEventEntry";
        private String globalIndexColumn = "globalIndex";
        private String timestampColumn = "timeStamp";
        private String eventIdentifierColumn = "eventIdentifier";
        private String aggregateIdentifierColumn = "aggregateIdentifier";
        private String sequenceNumberColumn = "sequenceNumber";
        private String typeColumn = "type";
        private String payloadTypeColumn = "payloadType";
        private String payloadRevisionColumn = "payloadRevision";
        private String payloadColumn = "payloadBuffer";
        private String metaDataColumn = "metaDataBuffer";
        private String countersTable = "Counters";
        private String eventLogTable = "EventLogEntry";
        private String batchIndexColumn = "batchIndex";
        private String nameColumn = "name";
        private String valueColumn = "value";

        public Builder withEventTable(String eventTable) {
            this.domainEventTable = eventTable;
            return this;
        }

        public Builder withSnapshotTable(String snapshotTable) {
            this.snapshotTable = snapshotTable;
            return this;
        }

        public Builder withGlobalIndexColumn(String globalIndexColumn) {
            this.globalIndexColumn = globalIndexColumn;
            return this;
        }

        public Builder withTimestampColumn(String timestampColumn) {
            this.timestampColumn = timestampColumn;
            return this;
        }

        public Builder withEventIdentifierColumn(String eventIdentifierColumn) {
            this.eventIdentifierColumn = eventIdentifierColumn;
            return this;
        }

        public Builder withAggregateIdentifierColumn(String aggregateIdentifierColumn) {
            this.aggregateIdentifierColumn = aggregateIdentifierColumn;
            return this;
        }

        public Builder withSequenceNumberColumn(String sequenceNumberColumn) {
            this.sequenceNumberColumn = sequenceNumberColumn;
            return this;
        }

        public Builder withTypeColumn(String typeColumn) {
            this.typeColumn = typeColumn;
            return this;
        }

        public Builder withPayloadTypeColumn(String payloadTypeColumn) {
            this.payloadTypeColumn = payloadTypeColumn;
            return this;
        }

        public Builder withPayloadRevisionColumn(String payloadRevisionColumn) {
            this.payloadRevisionColumn = payloadRevisionColumn;
            return this;
        }

        public Builder withPayloadColumn(String payloadColumn) {
            this.payloadColumn = payloadColumn;
            return this;
        }

        public Builder withMetaDataColumn(String metaDataColumn) {
            this.metaDataColumn = metaDataColumn;
            return this;
        }

        public Builder withCountersTable(String countersTable) {
            this.countersTable = countersTable;
            return this;
        }

        public Builder withEventLogTable(String eventLogTable) {
            this.eventLogTable = eventLogTable;
            return this;
        }

        public Builder withBatchIndexColumn(String batchIndexColumn) {
            this.batchIndexColumn = batchIndexColumn;
            return this;
        }

        public Builder withNameColumn(String nameColumn) {
            this.nameColumn = nameColumn;
            return this;
        }

        public Builder withValueColumn(String valueColumn) {
            this.valueColumn = valueColumn;
            return this;
        }

        public EventSchema build() {
            return new EventSchema(this);
        }
    }
}
