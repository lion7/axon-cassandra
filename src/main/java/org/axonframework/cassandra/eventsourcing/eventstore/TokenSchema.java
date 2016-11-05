package org.axonframework.cassandra.eventsourcing.eventstore;

public class TokenSchema {
    private final String tokenTable, processNameColumn, segmentColumn, tokenColumn, tokenTypeColumn, timeStampColumn;

    public TokenSchema() {
        this(builder());
    }

    private TokenSchema(Builder builder) {
        tokenTable = builder.tokenTable;
        processNameColumn = builder.processNameColumn;
        segmentColumn = builder.segmentColumn;
        tokenColumn = builder.tokenColumn;
        tokenTypeColumn = builder.tokenTypeColumn;
        timeStampColumn = builder.timeStampColumn;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String tokenTable() {
        return tokenTable;
    }

    public String processNameColumn() {
        return processNameColumn;
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
        return timeStampColumn;
    }

    @SuppressWarnings("SqlResolve")
    protected static class Builder {
        private String tokenTable = "TokenEntry";
        private String processNameColumn = "processName";
        private String segmentColumn = "segment";
        private String tokenColumn = "token";
        private String tokenTypeColumn = "tokenType";
        private String timeStampColumn = "timeStamp";

        public Builder withTokenTable(String tokenTable) {
            this.tokenTable = tokenTable;
            return this;
        }

        public Builder withProcessNameColumn(String processNameColumn) {
            this.processNameColumn = processNameColumn;
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

        public Builder withTimeStampColumn(String timeStampColumn) {
            this.timeStampColumn = timeStampColumn;
            return this;
        }

        public TokenSchema build() {
            return new TokenSchema(this);
        }
    }
}
