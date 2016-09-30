package org.axonframework.cassandra.eventsourcing.eventstore;

import org.axonframework.eventsourcing.eventstore.jdbc.EventSchema;

/**
 * Created by gle21221 on 13-9-2016.
 */
public class CassandraEventSchema extends EventSchema {

    private CassandraEventSchema() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends EventSchema.Builder {
        public Builder() {
            withPayloadColumn("payloadBuffer").withMetaDataColumn("metaDataBuffer");
        }
    }
}
