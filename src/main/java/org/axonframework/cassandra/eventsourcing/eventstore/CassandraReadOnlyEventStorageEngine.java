package org.axonframework.cassandra.eventsourcing.eventstore;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.BatchingEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.DomainEventData;
import org.axonframework.eventsourcing.eventstore.GlobalSequenceTrackingToken;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcaster;
import org.axonframework.serialization.xml.XStreamSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.axonframework.cassandra.eventsourcing.eventstore.EventSchema.quoted;
import static org.axonframework.common.ObjectUtils.getOrDefault;

public class CassandraReadOnlyEventStorageEngine extends BatchingEventStorageEngine {

    final Session session;
    final Mapper<DomainEventEntry> eventMapper;
    final Mapper<SnapshotEventEntry> snapshotMapper;
    final Mapper<EventLogEntry> eventLogMapper;
    private final EventSchema schema;

    /**
     * Initializes an EventStorageEngine with given {@code serializer}, {@code upcasterChain} and {@code
     * persistenceExceptionResolver}.
     *
     * @param serializer                   Used to serialize and deserialize event payload and metadata. If {@code null}
     *                                     an {@link XStreamSerializer} is used.
     * @param upcasterChain                Allows older revisions of serialized objects to be deserialized. If {@code
     *                                     null} a {@link org.axonframework.serialization.upcasting.event.NoOpEventUpcaster} is used.
     * @param persistenceExceptionResolver Detects concurrency exceptions from the backing database. If {@code null}
     *                                     persistence exceptions are not explicitly resolved.
     * @param batchSize                    The number of events that should be read at each database access. When more
     *                                     than this number of events must be read to rebuild an aggregate's state, the
     *                                     events are read in batches of this size. If {@code null} a batch size of 100
     *                                     is used. Tip: if you use a snapshotter, make sure to choose snapshot trigger
     *                                     and batch size such that a single batch will generally retrieve all events
     */
    public CassandraReadOnlyEventStorageEngine(Serializer serializer, EventUpcaster upcasterChain, PersistenceExceptionResolver persistenceExceptionResolver, Integer batchSize, Session session, EventSchema schema) {
        super(serializer, upcasterChain, persistenceExceptionResolver, batchSize);
        if (session == null) {
            throw new IllegalArgumentException("Parameter 'session' cannot be null");
        }
        MappingManager mappingManager = new MappingManager(session);
        this.session = session;
        this.schema = getOrDefault(schema, () -> EventSchema.builder().build());
        this.eventMapper = mappingManager.mapper(DomainEventEntry.class);
        this.snapshotMapper = mappingManager.mapper(SnapshotEventEntry.class);
        this.eventLogMapper = mappingManager.mapper(EventLogEntry.class);
    }

    @Override
    protected List<DomainEventEntry> fetchTrackedEvents(TrackingToken lastToken, int batchSize) {
        if (lastToken == null || lastToken instanceof GlobalSequenceTrackingToken) {
            long globalIndex = lastToken == null ? -1 : ((GlobalSequenceTrackingToken) lastToken).getGlobalIndex();
            long batchIndex = EventLogEntry.determineBatchIndex(globalIndex + 1);
            ResultSet resultSet = session.execute("SELECT " + quoted(schema().aggregateIdentifierColumn(), schema().sequenceNumberColumn()) +
                    " FROM " + quoted(schema().eventLogTable()) +
                    " WHERE " + quoted(schema().batchIndexColumn()) + " = ?" +
                    " AND " + quoted(schema().globalIndexColumn()) + " > ?" +
                    " ORDER BY " + quoted(schema().globalIndexColumn()) +
                    " LIMIT ?", batchIndex, globalIndex, EventLogEntry.BATCH_SIZE);
            // TODO: fetch a range by first determining the first and last sequence number of each aggregate
            return eventLogMapper.map(resultSet).all().stream()
                    .map(e -> eventMapper.get(e.getAggregateIdentifier(), e.getSequenceNumber()))
                    .collect(Collectors.toList());
        } else {
            throw new IllegalArgumentException(String.format("Token %s is of the wrong type", lastToken));
        }
    }

    @Override
    protected List<DomainEventEntry> fetchDomainEvents(String aggregateIdentifier, long firstSequenceNumber, int batchSize) {
        ResultSet resultSet = session.execute("SELECT " + quoted(domainEventFields()) +
                        " FROM " + quoted(schema().domainEventTable()) +
                        " WHERE " + quoted(schema().aggregateIdentifierColumn()) + " = ?" +
                        " AND " + quoted(schema().sequenceNumberColumn()) + " >= ?" +
                        " ORDER BY " + quoted(schema().sequenceNumberColumn()) + " ASC" +
                        " LIMIT ?",
                aggregateIdentifier,
                firstSequenceNumber,
                batchSize);
        return eventMapper.map(resultSet).all();
    }

    protected List<DomainEventEntry> fetchDomainEvents(String aggregateIdentifier, long firstSequenceNumber, long lastSequenceNumber, int batchSize) {
        ResultSet resultSet = session.execute("SELECT " + quoted(domainEventFields()) +
                        " FROM " + quoted(schema().domainEventTable()) +
                        " WHERE " + quoted(schema().aggregateIdentifierColumn()) + " = ?" +
                        " AND " + quoted(schema().sequenceNumberColumn()) + " >= ?" +
                        " AND " + quoted(schema().sequenceNumberColumn()) + " <= ?" +
                        " ORDER BY " + quoted(schema().sequenceNumberColumn()) + " ASC" +
                        " LIMIT ?",
                aggregateIdentifier,
                firstSequenceNumber,
                lastSequenceNumber,
                batchSize);
        return eventMapper.map(resultSet).all();
    }

    @Override
    protected void appendEvents(List<? extends EventMessage<?>> events, Serializer serializer) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void storeSnapshot(DomainEventMessage<?> snapshot, Serializer serializer) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Optional<? extends DomainEventData<?>> readSnapshotData(String aggregateIdentifier) {
        return Optional.ofNullable(snapshotMapper.get(aggregateIdentifier));
    }

    private EventSchema schema() {
        return schema;
    }

    private List<String> domainEventFields() {
        return Arrays.asList(schema().eventIdentifierColumn(), schema().timestampColumn(), schema().payloadTypeColumn(),
                schema().payloadRevisionColumn(), schema().payloadColumn(), schema().metaDataColumn(),
                schema().typeColumn(), schema().aggregateIdentifierColumn(), schema().sequenceNumberColumn(),
                schema().globalIndexColumn());
    }
}
