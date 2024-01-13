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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class Neo4jRelationshipsTable
        implements Neo4jTable
{
    private final Neo4jColumnHandle elementIdColumn;
    private final Neo4jColumnHandle typeColumn;
    private final Neo4jColumnHandle propertiesColumn;

    @Inject
    public Neo4jRelationshipsTable(Neo4jTypeManager typeManager)
    {
        this.elementIdColumn = new Neo4jColumnHandle("elementid", VARCHAR, false);
        this.typeColumn = new Neo4jColumnHandle("type", VARCHAR, false);
        this.propertiesColumn = new Neo4jColumnHandle("properties", typeManager.getJsonType(), false);
    }

    @Override
    public Neo4jTableHandle getTableHandle(String database)
    {
        return new Neo4jTableHandle(new Neo4jRelationshipsRelationHandle(Optional.ofNullable(database), Optional.empty(), OptionalLong.empty()));
    }

    public List<Neo4jColumnHandle> getColumns()
    {
        return ImmutableList.of(this.elementIdColumn, this.typeColumn, this.propertiesColumn);
    }

    public Neo4jColumnHandle getElementIdColumn()
    {
        return elementIdColumn;
    }

    public Neo4jColumnHandle getTypeColumn()
    {
        return typeColumn;
    }

    public Neo4jColumnHandle getPropertiesColumn()
    {
        return propertiesColumn;
    }
}
