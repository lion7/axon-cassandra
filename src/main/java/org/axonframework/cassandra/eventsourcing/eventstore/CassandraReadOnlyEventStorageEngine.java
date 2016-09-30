package org.axonframework.cassandra.eventsourcing.eventstore;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.BatchingEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.DomainEventData;
import org.axonframework.eventsourcing.eventstore.GlobalIndexTrackingToken;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.eventsourcing.eventstore.jdbc.EventSchema;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcasterChain;
import org.axonframework.serialization.upcasting.event.NoOpEventUpcasterChain;
import org.axonframework.serialization.xml.XStreamSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by gle21221 on 2-9-2016.
 */
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
     *                                     null} a {@link NoOpEventUpcasterChain} is used.
     * @param persistenceExceptionResolver Detects concurrency exceptions from the backing database. If {@code null}
     *                                     persistence exceptions are not explicitly resolved.
     * @param transactionManager           The transaction manager used to set the isolation level of the transaction
     *                                     when loading events.
     * @param batchSize                    The number of events that should be read at each database access. When more
     *                                     than this number of events must be read to rebuild an aggregate's state, the
     *                                     events are read in batches of this size. If {@code null} a batch size of 100
     *                                     is used. Tip: if you use a snapshotter, make sure to choose snapshot trigger
     *                                     and batch size such that a single batch will generally retrieve all events
     */
    public CassandraReadOnlyEventStorageEngine(Serializer serializer, EventUpcasterChain upcasterChain, PersistenceExceptionResolver persistenceExceptionResolver, TransactionManager transactionManager, Integer batchSize, Session session, EventSchema schema) {
        super(serializer, upcasterChain, persistenceExceptionResolver, transactionManager, batchSize);
        this.session = session;
        this.schema = schema;
        MappingManager mappingManager = new MappingManager(session);
        this.eventMapper = mappingManager.mapper(DomainEventEntry.class);
        this.snapshotMapper = mappingManager.mapper(SnapshotEventEntry.class);
        this.eventLogMapper = mappingManager.mapper(EventLogEntry.class);
    }

    static String quoted(String... input) {
        return quoted(Arrays.asList(input));
    }

    static String quoted(Iterable<String> input) {
        return '"' + String.join("\", \"", input) + '"';
    }

    @Override
    protected List<DomainEventEntry> fetchTrackedEvents(TrackingToken lastToken, int batchSize) {
        if (lastToken == null || lastToken instanceof GlobalIndexTrackingToken) {
            long globalIndex = lastToken == null ? -1 : ((GlobalIndexTrackingToken) lastToken).getGlobalIndex();
            long batchIndex = EventLogEntry.determineBatchIndex(globalIndex + 1);
            ResultSet resultSet = session.execute("SELECT " + quoted("eventsReferences") +
                    " FROM " + quoted("EventLogEntry") +
                    " WHERE " + quoted("batchIndex") + " = ?" +
                    " AND " + quoted("globalIndex") + " > ?" +
                    " ORDER BY " + quoted("globalIndex") +
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
    protected TrackingToken getTokenForGapDetection(TrackingToken token) {
        return token;
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
                "transactionIndex");
    }
}
