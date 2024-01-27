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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.Locale;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class Neo4jTableRelationHandle
        extends Neo4jRelationHandle
{
    public enum Type {
        NODES,
        RELATIONSHIPS;

        public String getTableName() {
            return this.name().toLowerCase(Locale.ENGLISH);
        }

        public static Type fromTableName(String tableName) {
            requireNonNull(tableName, "tableName is null");

            try {
                return Type.valueOf(tableName.toUpperCase(Locale.ENGLISH));
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    private final Type type;
    private final Optional<String> database;
    private final OptionalLong limit;
    private final TupleDomain<ColumnHandle> constraints;

    @JsonCreator
    public Neo4jTableRelationHandle(
            @JsonProperty("type") Type type,
            @JsonProperty("database") Optional<String> database,
            @JsonProperty("constraints") TupleDomain<ColumnHandle> constraints,
            @JsonProperty("limit") OptionalLong limit)

    {
        this.type = requireNonNull(type, "type is null");
        this.database = requireNonNull(database, "database is null");
        this.constraints = requireNonNull(constraints, "constraints is null");
        this.limit = limit;
    }

    @JsonProperty
    public Type getType() {
        return this.type;
    }

    @Override
    @JsonProperty
    public Optional<String> getDatabase()
    {
        return this.database;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraints()
    {
        return this.constraints;
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
                .add("constraints", constraints)
                .add("limit", limit)
                .toString();
    }
}
