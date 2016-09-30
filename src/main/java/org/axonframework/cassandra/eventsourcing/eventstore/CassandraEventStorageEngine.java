package org.axonframework.cassandra.eventsourcing.eventstore;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.EventUtils;
import org.axonframework.eventsourcing.eventstore.jdbc.EventSchema;
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork;
import org.axonframework.messaging.unitofwork.UnitOfWork;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcasterChain;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by gle21221 on 2-9-2016.
 */
public class CassandraEventStorageEngine extends CassandraReadOnlyEventStorageEngine {

    private static final String EVENTS_COUNTER_NAME = "events";
    private final String batchKey = this + "_BATCH";

    private final AtomicLong globalIndexCounter;

    public CassandraEventStorageEngine(Serializer serializer, EventUpcasterChain upcasterChain, PersistenceExceptionResolver persistenceExceptionResolver, TransactionManager transactionManager, Integer batchSize, Session session, EventSchema schema) {
        super(serializer, upcasterChain, persistenceExceptionResolver, transactionManager, batchSize, session, schema);
        this.globalIndexCounter = new AtomicLong(selectCounter(EVENTS_COUNTER_NAME));
    }

    private static DomainEventEntry asDomainEventEntry(DomainEventMessage<?> eventMessage, Serializer serializer, long globalIndex) {
        return new DomainEventEntry(globalIndex, eventMessage, serializer);
    }

    private static SnapshotEventEntry asSnapshotEventEntry(DomainEventMessage<?> eventMessage, Serializer serializer) {
        return new SnapshotEventEntry(eventMessage, serializer);
    }

    @Override
    protected void appendEvents(List<? extends EventMessage<?>> events, Serializer serializer) {
        events.stream()
                .map(EventUtils::asDomainEventMessage)
                .map(e -> asDomainEventEntry(e, serializer, globalIndexCounter.getAndIncrement()))
                .map(this::storeEventLogEntry)
                .map(eventMapper::saveQuery)
                .forEachOrdered(batch()::add);
    }

    @Override
    protected void storeSnapshot(DomainEventMessage<?> snapshot, Serializer serializer) {
        batch().add(snapshotMapper.saveQuery(asSnapshotEventEntry(snapshot, serializer)));
    }

    private DomainEventEntry storeEventLogEntry(DomainEventEntry event) {
        EventLogEntry eventLogEntry = new EventLogEntry(event.getGlobalIndex(), event.getAggregateIdentifier(), event.getSequenceNumber());
        batch().add(eventLogMapper.saveQuery(eventLogEntry));
        return event;
    }

    private BatchStatement batch() {
        UnitOfWork<?> root = CurrentUnitOfWork.get().root();
        return root.getOrComputeResource(batchKey, s -> {
            BatchStatement batch = new BatchStatement();
            root.onCommit(unitOfWork -> session.execute(batch));
            return batch;
        });
    }

    private long selectCounter(String name) {
        ResultSet resultSet = session.execute("SELECT " + quoted("value") +
                " FROM" + quoted("Counters") +
                " WHERE " + quoted("name") + " = ? LIMIT 1", name);
        return Optional.ofNullable(resultSet.one()).map(row -> row.getLong(0)).orElse(0L);
    }

}
