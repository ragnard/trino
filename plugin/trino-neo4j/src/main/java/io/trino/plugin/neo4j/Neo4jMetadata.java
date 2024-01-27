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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.neo4j.ptf.Query;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.TupleDomain;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class Neo4jMetadata
        implements ConnectorMetadata
{
    private final Neo4jClient client;
    private final Neo4jTypeManager typeManager;
    private final Neo4jTables tables;

    @Inject
    public Neo4jMetadata(
            Neo4jClient client,
            Neo4jTypeManager typeManager,
            Neo4jTables tables)
    {
        this.client = requireNonNull(client, "client is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tables = requireNonNull(tables, "tables is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return this.client.listSchemaNames(session);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return this.tables.getTableNames()
                .stream()
                .map(name -> new SchemaTableName(schemaName.orElseThrow(), name))
                .collect(toImmutableList());
    }

    @Nullable
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        Neo4jTableRelationHandle.Type type = Neo4jTableRelationHandle.Type.fromTableName(schemaTableName.getTableName());
        if (type == null) {
            return null;
        }

        Neo4jTable table = this.tables.get(type);
        if (table == null) {
            return null;
        }

        return table.getTableHandle(schemaTableName.getSchemaName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Neo4jTableHandle handle = (Neo4jTableHandle) tableHandle;

        return switch (handle.getRelationHandle()) {
            case Neo4jQueryRelationHandle queryRelationHandle -> {
                List<ColumnMetadata> columnMetadata = queryRelationHandle.getDescriptor()
                        .getFields()
                        .stream()
                        .map(f -> new ColumnMetadata(f.getName().orElseThrow(), f.getType().orElseThrow()))
                        .collect(toImmutableList());

                yield new ConnectorTableMetadata(
                        new SchemaTableName("_generated", "_generated_query"),
                        columnMetadata);
            }
            case Neo4jTableRelationHandle tableRelationHandle -> {
                Neo4jTableRelationHandle.Type type = tableRelationHandle.getType();
                Neo4jTable table = this.tables.get(type);
                if (table == null) {
                    yield null;
                }

                SchemaTableName schemaTableName = new SchemaTableName(
                        tableRelationHandle.getDatabase().orElseThrow(),
                        type.getTableName());

                yield new ConnectorTableMetadata(
                        schemaTableName,
                        table.getColumns()
                                .stream()
                                .map(Neo4jColumnHandle::toColumnMetadata)
                                .collect(Collectors.toList()));
            }
        };
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Neo4jTableHandle handle = (Neo4jTableHandle) tableHandle;
        Neo4jRelationHandle relationHandle = handle.getRelationHandle();

        return switch (relationHandle) {
            case Neo4jTableRelationHandle table -> this.tables.get(table.getType())
                    .getColumns()
                    .stream()
                    .collect(toImmutableMap(Neo4jColumnHandle::getColumnName, c -> c));
            default -> ImmutableMap.of();
        };
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((Neo4jColumnHandle) columnHandle).toColumnMetadata();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        Neo4jTableHandle handle = (Neo4jTableHandle) tableHandle;

        return switch (handle.getRelationHandle()) {
            case Neo4jTableRelationHandle h -> {
                TupleDomain<ColumnHandle> oldDomain = h.getConstraints();
                TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
                TupleDomain<ColumnHandle> remainingFilter;

                if (newDomain.isNone()) {
                    remainingFilter = TupleDomain.all();
                }
                else {
                    Neo4jTable table = this.tables.get(h.getType());
                    Neo4jTable.PushdownResult pushdownResult = table.pushDown(constraint);

                    newDomain = pushdownResult.newDomain();
                    remainingFilter = pushdownResult.remainingFilter();
                }

                if (oldDomain.equals(newDomain)) {
                    yield Optional.empty();
                }

                Neo4jTableRelationHandle newHandle = new Neo4jTableRelationHandle(
                        h.getType(),
                        h.getDatabase(),
                        newDomain,
                        h.getLimit());

                yield Optional.of(new ConstraintApplicationResult<>(new Neo4jTableHandle(newHandle),
                        remainingFilter, constraint.getExpression(), false));
            }
            default -> Optional.empty();
        };
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle tableHandle, long limit)
    {
        Neo4jTableHandle handle = (Neo4jTableHandle) tableHandle;

        return switch (handle.getRelationHandle()) {
            case Neo4jTableRelationHandle tableRelationHandle -> {
                Neo4jTableRelationHandle newHandle = new Neo4jTableRelationHandle(
                        tableRelationHandle.getType(),
                        tableRelationHandle.getDatabase(),
                        tableRelationHandle.getConstraints(),
                        OptionalLong.of(limit));

                yield Optional.of(new LimitApplicationResult<>(new Neo4jTableHandle(newHandle), true, true));
            }
            default -> Optional.empty();
        };
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (!(handle instanceof Query.QueryFunctionHandle queryFunctionHandle)) {
            return Optional.empty();
        }

        Neo4jQueryRelationHandle queryHandle = queryFunctionHandle.getQueryHandle();

        if (this.typeManager.isDynamicResultDescriptor(queryHandle.getDescriptor())) {
            return Optional.of(new TableFunctionApplicationResult<>(new Neo4jTableHandle(queryHandle),
                    List.of(this.typeManager.getDynamicResultColumn())));
        }
        else {
            List<ColumnHandle> columnHandles = queryHandle.getDescriptor()
                    .getFields().stream()
                    .map(f -> new Neo4jColumnHandle(f.getName().orElseThrow(), f.getType().orElseThrow(), true))
                    .collect(toImmutableList());

            return Optional.of(new TableFunctionApplicationResult<>(new Neo4jTableHandle(queryHandle), columnHandles));
        }
    }
}
