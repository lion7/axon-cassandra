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

    public CassandraTokenStore(Session session, Serializer serializer) {
        if (session == null) {
            throw new IllegalArgumentException("Parameter 'session' cannot be null");
        }
        MappingManager mappingManager = new MappingManager(session);
        this.tokenMapper = mappingManager.mapper(TokenEntry.class);
        this.serializer = getOrDefault(serializer, XStreamSerializer::new);
    }

    @Override
    public void storeToken(TrackingToken token, String processName, int segment) {
        tokenMapper.save(new TokenEntry(processName, segment, token, serializer));
    }

    @Override
    public TrackingToken fetchToken(String processName, int segment) {
        Optional<TokenEntry> tokenEntry = Optional.ofNullable(tokenMapper.get(processName, segment));
        return tokenEntry.map(entry -> entry.trackingToken(serializer)).orElse(null);
    }
}
