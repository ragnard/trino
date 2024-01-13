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
import org.neo4j.cypherdsl.core.Relationship;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static org.neo4j.cypherdsl.core.Conditions.noCondition;

public class Neo4jRelationshipsRelationHandle
        extends Neo4jRelationHandle
{
    private final Optional<String> database;
    private final Optional<String> type;
    private final OptionalLong limit;

    @JsonCreator
    public Neo4jRelationshipsRelationHandle(
            @JsonProperty("database") Optional<String> database,
            @JsonProperty("type") Optional<String> type,
            @JsonProperty("limit") OptionalLong limit)

    {
        this.database = requireNonNull(database, "database is null");
        this.type = type;
        this.limit = limit;
    }

    @Override
    @JsonProperty
    public Optional<String> getDatabase()
    {
        return this.database;
    }

    @JsonProperty
    public Optional<String> getType()
    {
        return this.type;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return this.limit;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("database", database)
                .add("type", type)
                .add("limit", limit)
                .toString();
    }

    public String getQuery(List<Neo4jColumnHandle> columnHandles)
    {
        Relationship r = Cypher.anyNode().relationshipBetween(Cypher.anyNode(), this.type.orElse(null));

        List<AliasedExpression> columns = columnHandles.stream()
                .map(c -> switch (c.getColumnName()) {
                    case "elementid" -> Cypher.elementId(r).as(c.getColumnName());
                    case "type" -> Cypher.type(r).as(c.getColumnName());
                    case "properties" -> Cypher.properties(r).as(c.getColumnName());
                    default -> throw new IllegalStateException("Unexpected column: " + c.getColumnName());
                })
                .collect(Collectors.toList());

        return Cypher.match(r)
                .where(noCondition())
                .returning(columns)
                .limit(this.limit.orElse(10_000))
                .build()
                .getCypher();
    }
}
