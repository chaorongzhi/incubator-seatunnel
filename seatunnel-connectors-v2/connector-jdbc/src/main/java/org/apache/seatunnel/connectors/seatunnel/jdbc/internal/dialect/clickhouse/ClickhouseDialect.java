package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.clickhouse;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.util.Optional;

public class ClickhouseDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return DatabaseIdentifier.CLICKHOUSE;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new ClickhouseJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new ClickhouseTypeMapper();
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.empty();
    }
}
