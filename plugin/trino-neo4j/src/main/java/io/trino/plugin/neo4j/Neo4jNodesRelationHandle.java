/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.neo4j;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.neo4j.cypherdsl.core.AliasedExpression;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.core.Node;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static org.neo4j.cypherdsl.core.Conditions.noCondition;

public class Neo4jNodesRelationHandle
        extends Neo4jRelationHandle
{
    private final String database;
    private final List<String> labels;
    private final OptionalLong limit;

    @JsonCreator
    public Neo4jNodesRelationHandle(
            @JsonProperty("database") String database,
            @JsonProperty("labels") List<String> labels,
            @JsonProperty("limit") OptionalLong limit)

    {
        this.database = requireNonNull(database, "database is null");
        this.labels = requireNonNull(labels, "labels is null");
        this.limit = limit;
    }

    @JsonProperty
    public String getDatabase()
    {
        return this.database;
    }

    @JsonProperty
    public List<String> getLabels()
    {
        return this.labels;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return this.limit;
    }

    @Override
    public Optional<String> getDatabaseName()
    {
        return Optional.of(this.database);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("database", database)
                .add("labels", labels)
                .add("limit", limit)
                .toString();
    }

    public String getQuery(List<Neo4jColumnHandle> columnHandles)
    {
        // TODO: all labels
        Node node = this.labels.isEmpty() ? Cypher.anyNode() : Cypher.node(this.labels.get(0));

        List<AliasedExpression> columns = columnHandles.stream()
                .map(c -> switch (c.getColumnName()) {
                    case "elementId" -> Cypher.elementId(node).as(c.getColumnName());
                    case "labels" -> Cypher.labels(node).as(c.getColumnName());
                    case "properties" -> Cypher.properties(node).as(c.getColumnName());
                    default -> throw new IllegalStateException("Unexpected column: " + c.getColumnName());
                })
                .collect(Collectors.toList());

        return Cypher.match(node)
                .where(noCondition())
                .returning(columns)
                .limit(this.limit.orElse(10_000))
                .build()
                .getCypher();
    }
}
