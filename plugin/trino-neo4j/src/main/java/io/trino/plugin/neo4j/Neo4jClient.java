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
import com.google.inject.ConfigurationException;
import com.google.inject.Inject;
import com.google.inject.spi.Message;
import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorSession;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Locale.ENGLISH;

public class Neo4jClient
        implements AutoCloseable
{
    private static final Logger log = Logger.get(Neo4jClient.class);

    private static final String SYSTEM_DATABASE = "system";
    private static final String DEFAULT_DATABASE = "neo4j";

    private final Driver driver;
    private final Neo4jTypeManager typeManager;
    private final Neo4jStatementBuilder statementBuilder;
    private final Neo4jTables tables;

    @Inject
    public Neo4jClient(
            Neo4jConnectorConfig config,
            Neo4jTypeManager typeManager,
            Neo4jStatementBuilder statementBuilder,
            Neo4jTables tables)
    {
        this.driver = GraphDatabase.driver(config.getURI(), getAuthToken(config));
        this.typeManager = typeManager;
        this.statementBuilder = statementBuilder;
        this.tables = tables;
    }

    private static AuthToken getAuthToken(Neo4jConnectorConfig config)
    {
        return switch (config.getAuthType().toLowerCase(ENGLISH)) {
            case "basic" -> AuthTokens.basic(config.getBasicAuthUser(), config.getBasicAuthPassword());
            case "bearer" -> AuthTokens.bearer(config.getBearerAuthToken());
            case "none" -> AuthTokens.none();
            default -> throw new ConfigurationException(ImmutableList.of(new Message("Unknown neo4j.auth.type: %s".formatted(config.getAuthType()))));
        };
    }

    public List<String> listSchemaNames(ConnectorSession connectorSession)
    {
        try (Session session = this.driver.session(SessionConfig.forDatabase(SYSTEM_DATABASE))) {
            return session.executeRead(tx -> {
                return tx.run("SHOW DATABASES YIELD name, type WHERE type <> 'system' RETURN distinct(name)")
                        .stream()
                        .map(r -> r.get(0).asString())
                        .map(s -> s.toLowerCase(ENGLISH))
                        .collect(toImmutableList());
            });
        }
    }

    public String toCypher(Neo4jTableHandle table, List<Neo4jColumnHandle> columnHandles)
    {
        return switch (table.getRelationHandle()) {
            case Neo4jQueryRelationHandle handle -> handle.getQuery();
            case Neo4jTableRelationHandle handle -> this.tables.get(handle.getType()).toCypherQuery(handle, columnHandles);
        };
    }

    @Override
    public void close()
            throws Exception
    {
        this.driver.close();
    }

    public Session newSession(Optional<String> databaseName)
    {
        return this.driver.session(databaseName
                .map(SessionConfig::forDatabase)
                .orElse(SessionConfig.defaultConfig()));
    }

    private void logQuery(String databaseName, String cypher)
    {
        logQuery(databaseName, cypher, Map.of());
    }

    private void logQuery(String databaseName, String cypher, Map<String, Object> parameters)
    {
        log.debug("Executing query: database=%s cypher=%s parameters=%s ", databaseName, cypher, parameters);
    }
}
