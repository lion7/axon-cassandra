package org.axonframework.cassandra.eventsourcing.eventstore;

import com.datastax.driver.core.Session;

import java.util.Arrays;

public class TokenSchema {
    private final String tokenTable, processorNameColumn, segmentColumn, tokenColumn, tokenTypeColumn, timestampColumn, ownerColumn;

    public TokenSchema() {
        this(builder());
    }

    private TokenSchema(Builder builder) {
        tokenTable = builder.tokenTable;
        processorNameColumn = builder.processorNameColumn;
        segmentColumn = builder.segmentColumn;
        tokenColumn = builder.tokenColumn;
        tokenTypeColumn = builder.tokenTypeColumn;
        timestampColumn = builder.timestampColumn;
        ownerColumn = builder.ownerColumn;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String tokenTable() {
        return tokenTable;
    }

    public String processorNameColumn() {
        return processorNameColumn;
    }

    public String segmentColumn() {
        return segmentColumn;
    }

    public String tokenColumn() {
        return tokenColumn;
    }

    public String tokenTypeColumn() {
        return tokenTypeColumn;
    }

    public String timestampColumn() {
        return timestampColumn;
    }

    public String ownerColumn() {
        return ownerColumn;
    }

    public void initialize(Session session) {
        session.execute(initializeTokenTable());
    }

    private String initializeTokenTable() {
        return "CREATE TABLE IF NOT EXISTS " + quoted(tokenTable) + " (" +
                quoted(processorNameColumn) + " varchar, " +
                quoted(segmentColumn) + " int, " +
                quoted(tokenColumn) + " blob, " +
                quoted(tokenTypeColumn) + " varchar, " +
                quoted(timestampColumn) + " timestamp, " +
                quoted(ownerColumn) + " varchar, " +
                "PRIMARY KEY(" + quoted(processorNameColumn) + ", " + quoted(segmentColumn) + ")" +
                ");";
    }

    static String quoted(String... input) {
        return quoted(Arrays.asList(input));
    }

    static String quoted(Iterable<String> input) {
        return '"' + String.join("\", \"", input) + '"';
    }

    @SuppressWarnings("SqlResolve")
    protected static class Builder {
        private String tokenTable = "TokenEntry";
        private String processorNameColumn = "processorName";
        private String segmentColumn = "segment";
        private String tokenColumn = "token";
        private String tokenTypeColumn = "tokenType";
        private String timestampColumn = "timestamp";
        private String ownerColumn = "owner";

        public Builder withTokenTable(String tokenTable) {
            this.tokenTable = tokenTable;
            return this;
        }

        public Builder withProcessorNameColumn(String processorNameColumn) {
            this.processorNameColumn = processorNameColumn;
            return this;
        }

        public Builder withSegmentColumn(String segmentColumn) {
            this.segmentColumn = segmentColumn;
            return this;
        }

        public Builder withTokenColumn(String tokenColumn) {
            this.tokenColumn = tokenColumn;
            return this;
        }

        public Builder withTokenTypeColumn(String tokenTypeColumn) {
            this.tokenTypeColumn = tokenTypeColumn;
            return this;
        }

        public Builder withTimestampColumn(String timestampColumn) {
            this.timestampColumn = timestampColumn;
            return this;
        }

        public Builder withOwnerColumn(String ownerColumn) {
            this.ownerColumn = ownerColumn;
            return this;
        }

        public TokenSchema build() {
            return new TokenSchema(this);
        }
    }
}
