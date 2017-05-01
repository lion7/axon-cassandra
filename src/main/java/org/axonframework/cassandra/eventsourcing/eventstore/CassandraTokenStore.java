package org.axonframework.cassandra.eventsourcing.eventstore;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.XStreamSerializer;

import java.util.Optional;

import static org.axonframework.common.ObjectUtils.getOrDefault;

public class CassandraTokenStore implements TokenStore {

    private final Serializer serializer;
    private final Mapper<TokenEntry> tokenMapper;
    private final TokenSchema schema;

    public CassandraTokenStore(Session session, Serializer serializer, TokenSchema schema) {
        if (session == null) {
            throw new IllegalArgumentException("Parameter 'session' cannot be null");
        }
        MappingManager mappingManager = new MappingManager(session);
        this.tokenMapper = mappingManager.mapper(TokenEntry.class);
        this.serializer = getOrDefault(serializer, XStreamSerializer::new);
        this.schema = getOrDefault(schema, TokenSchema.builder().build());
    }

    @Override
    public void storeToken(TrackingToken token, String processorName, int segment) {
        tokenMapper.save(new TokenEntry(processorName, segment, token, serializer));
    }

    @Override
    public TrackingToken fetchToken(String processorName, int segment) {
        Optional<TokenEntry> tokenEntry = Optional.ofNullable(tokenMapper.get(processorName, segment));
        return tokenEntry.map(entry -> entry.trackingToken(serializer)).orElse(null);
    }

    @Override
    public void releaseClaim(String processorName, int segment) {
        Optional<TokenEntry> tokenEntry = Optional.ofNullable(tokenMapper.get(processorName, segment));
        tokenEntry.ifPresent(tokenMapper::delete);
    }
}
