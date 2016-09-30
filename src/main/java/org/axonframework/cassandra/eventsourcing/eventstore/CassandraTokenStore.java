package org.axonframework.cassandra.eventsourcing.eventstore;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.serialization.Serializer;

import java.util.Optional;

/**
 * Created by gle21221 on 2-9-2016.
 */
public class CassandraTokenStore implements TokenStore {

    private final Serializer serializer;
    private final Mapper<TokenEntry> tokenMapper;

    public CassandraTokenStore(Session session, Serializer serializer) {
        this.serializer = serializer;
        MappingManager mappingManager = new MappingManager(session);
        this.tokenMapper = mappingManager.mapper(TokenEntry.class);
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
