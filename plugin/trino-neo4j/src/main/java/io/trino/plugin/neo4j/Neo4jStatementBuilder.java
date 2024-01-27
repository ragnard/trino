package io.trino.plugin.neo4j;

import com.google.inject.Inject;

import static java.util.Objects.requireNonNull;

public class Neo4jStatementBuilder
{
    private final Neo4jTables tables;

    @Inject
    public Neo4jStatementBuilder(
            Neo4jTables tables)
    {
        this.tables = requireNonNull(tables, "tables is null");
    }
}
