package io.trino.plugin.neo4j;

import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class Neo4jTables
{
    private final Map<Neo4jTableRelationHandle.Type, Neo4jTable> tables;

    @Inject
    public Neo4jTables(Neo4jTypeManager typeManager)
    {
        this.tables = Stream.of(
                        new Neo4jNodesTable(typeManager),
                        new Neo4jRelationshipsTable(typeManager))
                .collect(toImmutableMap(Neo4jTable::getTableType, Function.identity()));
    }

    public List<String> getTableNames() {
        return this.tables.keySet()
                .stream()
                .map(Neo4jTableRelationHandle.Type::getTableName)
                .collect(toImmutableList());
    }

    public Neo4jTable get(Neo4jTableRelationHandle.Type type)
    {
        return this.tables.get(type);
    }
}
