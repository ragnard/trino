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

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;

public interface Neo4jTable
{
    Neo4jTableRelationHandle.Type getTableType();

    Neo4jTableHandle getTableHandle(String database);

    List<Neo4jColumnHandle> getColumns();

    record PushdownResult(TupleDomain<ColumnHandle> newDomain, TupleDomain<ColumnHandle> remainingFilter) {}

    PushdownResult pushDown(Constraint constraint);

    String toCypherQuery(Neo4jTableRelationHandle handle, List<Neo4jColumnHandle> columnHandles);
}
