package org.axonframework.cassandra.eventsourcing.eventstore;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.EventUtils;
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork;
import org.axonframework.messaging.unitofwork.UnitOfWork;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcasterChain;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.axonframework.cassandra.eventsourcing.eventstore.EventSchema.quoted;

public class CassandraEventStorageEngine extends CassandraReadOnlyEventStorageEngine {

    private static final String GLOBAL_INDEX_COUNTER_NAME = "globalIndex";
    private final String batchKey = this + "_BATCH";

    private final AtomicLong globalIndexCounter;
    private final PreparedStatement counterSelectStatement;
    private final PreparedStatement counterInsertStatement;

    public CassandraEventStorageEngine(Serializer serializer, EventUpcasterChain upcasterChain, PersistenceExceptionResolver persistenceExceptionResolver, TransactionManager transactionManager, Integer batchSize, Session session, EventSchema schema) {
        super(serializer, upcasterChain, persistenceExceptionResolver, transactionManager, batchSize, session, schema);

        schema.initialize(session);

        this.counterSelectStatement = session.prepare("SELECT " + quoted(schema.valueColumn()) +
                " FROM" + quoted(schema.countersTable()) +
                " WHERE " + quoted(schema.nameColumn()) + " = ? LIMIT 1");
        this.counterInsertStatement = session.prepare("INSERT INTO " + quoted(schema.countersTable()) +
                " (" + quoted(schema.nameColumn(), schema.valueColumn()) + ")" +
                " VALUES(?,?)");

        Row globalIndexCounterRow = session.execute(counterSelectStatement.bind(GLOBAL_INDEX_COUNTER_NAME)).one();
        this.globalIndexCounter = new AtomicLong(Optional.ofNullable(globalIndexCounterRow)
                .map(row -> row.getLong(0))
                .orElse(0L));
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
        batch().add(counterInsertStatement.bind(GLOBAL_INDEX_COUNTER_NAME, globalIndexCounter.get()));
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

}
