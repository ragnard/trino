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
import io.trino.spi.connector.Constraint;
import io.trino.spi.predicate.TupleDomain;
import org.neo4j.cypherdsl.core.AliasedExpression;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.core.Relationship;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.neo4j.cypherdsl.core.Conditions.noCondition;

public class Neo4jRelationshipsTable
        implements Neo4jTable
{
    private final Neo4jColumnHandle elementIdColumn;
    //private final Neo4jColumnHandle startElementIdColumn;
    //private final Neo4jColumnHandle endElementIdColumn;
    private final Neo4jColumnHandle typeColumn;
    private final Neo4jColumnHandle propertiesColumn;

    @Inject
    public Neo4jRelationshipsTable(Neo4jTypeManager typeManager)
    {
        this.elementIdColumn = new Neo4jColumnHandle("element_id", VARCHAR, false);
        //this.startElementIdColumn = new Neo4jColumnHandle("start_element_id", VARCHAR, false);
        //this.endElementIdColumn = new Neo4jColumnHandle("end_element_id", VARCHAR, false);
        this.typeColumn = new Neo4jColumnHandle("type", VARCHAR, false);
        this.propertiesColumn = new Neo4jColumnHandle("properties", typeManager.getJsonType(), false);
    }

    @Override
    public Neo4jTableRelationHandle.Type getTableType()
    {
        return Neo4jTableRelationHandle.Type.RELATIONSHIPS;
    }

    @Override
    public Neo4jTableHandle getTableHandle(String database)
    {
        return new Neo4jTableHandle(new Neo4jTableRelationHandle(
                getTableType(),
                Optional.ofNullable(database),
                TupleDomain.all(),
                OptionalLong.empty()));
    }

    public List<Neo4jColumnHandle> getColumns()
    {
        return ImmutableList.of(this.elementIdColumn, this.typeColumn, this.propertiesColumn);
    }

    @Override
    public PushdownResult pushDown(Constraint constraint)
    {
        return new PushdownResult(TupleDomain.all(), constraint.getSummary());
    }

    @Override
    public String toCypherQuery(Neo4jTableRelationHandle handle, List<Neo4jColumnHandle> columnHandles)
    {
        Relationship r = Cypher.anyNode().relationshipBetween(Cypher.anyNode());

        List<AliasedExpression> columns = columnHandles.stream()
                .map(c -> switch (c.getColumnName()) {
                    case "element_id" -> Cypher.elementId(r).as(c.getColumnName());
                    case "type" -> Cypher.type(r).as(c.getColumnName());
                    case "properties" -> Cypher.properties(r).as(c.getColumnName());
                    default -> throw new IllegalStateException("Unexpected column: " + c.getColumnName());
                })
                .collect(Collectors.toList());

        return Cypher.match(r)
                .where(noCondition())
                .returning(columns)
                .limit(handle.getLimit().orElse(10_000))
                .build()
                .getCypher();
    }

}
