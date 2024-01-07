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
import io.trino.spi.type.ArrayType;

import java.util.List;
import java.util.OptionalLong;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class Neo4jNodesTable
{
    private final Neo4jColumnHandle elementIdColumn;
    private final Neo4jColumnHandle labelsColumn;
    private final Neo4jColumnHandle propertiesColumn;

    @Inject
    public Neo4jNodesTable(Neo4jTypeManager typeManager)
    {
        this.elementIdColumn = new Neo4jColumnHandle("elementId", VARCHAR, false);
        this.labelsColumn = new Neo4jColumnHandle("labels", new ArrayType(VARCHAR), false);
        this.propertiesColumn = new Neo4jColumnHandle("properties", typeManager.getJsonType(), false);
    }

    public Neo4jTable getNodesTable(String database)
    {
        List<Neo4jColumnHandle> columns = ImmutableList.of(this.elementIdColumn, this.labelsColumn, this.propertiesColumn);

        return new Neo4jTable(
                new Neo4jTableHandle(new Neo4jNodesRelationHandle(database, ImmutableList.of(), OptionalLong.empty())),
                columns);
    }

    public Neo4jColumnHandle getElementIdColumn()
    {
        return elementIdColumn;
    }

    public Neo4jColumnHandle getLabelsColumn()
    {
        return labelsColumn;
    }

    public Neo4jColumnHandle getPropertiesColumn()
    {
        return propertiesColumn;
    }
}
