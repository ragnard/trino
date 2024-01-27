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
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import org.neo4j.cypherdsl.core.AliasedExpression;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.core.LabelExpression;
import org.neo4j.cypherdsl.core.Node;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.neo4j.cypherdsl.core.Conditions.noCondition;

public class Neo4jNodesTable
        implements Neo4jTable
{
    private final Neo4jColumnHandle elementIdColumn;
    private final Neo4jColumnHandle labelsColumn;
    private final Neo4jColumnHandle propertiesColumn;

    @Inject
    public Neo4jNodesTable(Neo4jTypeManager typeManager)
    {
        this.elementIdColumn = new Neo4jColumnHandle("element_id", VARCHAR, false);
        this.labelsColumn = new Neo4jColumnHandle("labels", new ArrayType(VARCHAR), false);
        this.propertiesColumn = new Neo4jColumnHandle("properties", typeManager.getJsonType(), false);
    }

    @Override
    public Neo4jTableRelationHandle.Type getTableType()
    {
        return Neo4jTableRelationHandle.Type.NODES;
    }

    @Override
    public Neo4jTableHandle getTableHandle(String database)
    {
        return new Neo4jTableHandle(
                new Neo4jTableRelationHandle(
                        getTableType(),
                        Optional.ofNullable(database),
                        TupleDomain.all(),
                        OptionalLong.empty()));
    }

    public List<Neo4jColumnHandle> getColumns()
    {
        return ImmutableList.of(this.elementIdColumn, this.labelsColumn, this.propertiesColumn);
    }

    @Override
    public PushdownResult pushDown(Constraint constraint)
    {
        ConnectorExpression expression = constraint.getExpression();

        // ConnectorExpressions.extractConjuncts()

        switch (expression) {
            case Call c -> {
                switch (c.getFunctionName().getName()) {
                    case "contains" -> {}
                }
            }
            default -> {}
        }


        Map<ColumnHandle, Domain> supported = new HashMap<>();
        Map<ColumnHandle, Domain> unsupported = new HashMap<>();

        constraint.getSummary()
                .getDomains()
                .stream()
                .flatMap(x -> x.entrySet().stream())
                .forEach(x -> {
                    if (x.getKey().equals(this.labelsColumn)) {
                        if (x.getValue().getValues().isDiscreteSet()) {
                            supported.put(x.getKey(), x.getValue());
                        }
                    }
                    else {
                        unsupported.put(x.getKey(), x.getValue());
                    }
                });

        return new PushdownResult(
                TupleDomain.withColumnDomains(supported),
                TupleDomain.withColumnDomains(unsupported));
    }

    @Override
    public String toCypherQuery(Neo4jTableRelationHandle handle, List<Neo4jColumnHandle> columnHandles)
    {
        Node node = getConstrainedNode(handle.getConstraints());

        List<AliasedExpression> columns = columnHandles.stream()
                .map(c -> switch (c.getColumnName()) {
                    case "element_id" -> Cypher.elementId(node).as(c.getColumnName());
                    case "labels" -> Cypher.labels(node).as(c.getColumnName());
                    case "properties" -> Cypher.properties(node).as(c.getColumnName());
                    default -> throw new IllegalStateException("Unexpected column: " + c.getColumnName());
                })
                .collect(Collectors.toList());

        return Cypher.match(node)
                .where(noCondition())
                .returning(columns)
                .limit(handle.getLimit().orElse(10_000))
                .build()
                .getCypher();
    }

    private Node getConstrainedNode(TupleDomain<ColumnHandle> constraints) {
        Domain domain = constraints.getDomain(this.labelsColumn, this.labelsColumn.getColumnType());
        if (domain.isAll()) {
            return Cypher.anyNode();
        }

        domain.getSingleValue();

        // new ArrayType(VARCHAR).getObject()

        ImmutableList<Object> labels = domain.getValues().getDiscreteSet()
                .stream()
                .map(Block.class::cast)
                .map(b -> this.labelsColumn.getColumnType().getObject(b, 0))
                .map(o -> o)
                .collect(toImmutableList());


        return Cypher.node(new LabelExpression(LabelExpression.Type.LEAF, false, List.of("Question"), null, null));
    }
}
