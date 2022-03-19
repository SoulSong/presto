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
package com.facebook.presto.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.facebook.airlift.log.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AbstractDynamoDbTest {
    private static final Logger log = Logger.get(AbstractDynamoDbTest.class);

    protected static final DynamoDBMapper dynamoDbMapper;
    protected static final DynamoDbConfig dynamoDbConfig;
    protected static final AmazonDynamoDBClient dynamoDbClient;

    static {
        dynamoDbConfig = new DynamoDbConfig();
        dynamoDbConfig.setAccessKey("access_key_id");
        dynamoDbConfig.setSecretKey("secret_key_id");
        dynamoDbConfig.setRegion("us-east-1");
        dynamoDbConfig.setEndpoint("http://127.0.0.1:8000");

        dynamoDbMapper = DynamoDbClientFactory.dynamoDbMapper(dynamoDbConfig);
        dynamoDbClient = (AmazonDynamoDBClient) DynamoDbClientFactory.dynamoDbClient(dynamoDbConfig);
    }


    protected void execute(QueryRequest queryRequest, List<Map<String, AttributeValue>> results) {
        queryRequest.withExclusiveStartKey(null);
        do {
            QueryResult result = dynamoDbClient.query(queryRequest);
            results.addAll(result.getItems());

            queryRequest.withExclusiveStartKey(result.getLastEvaluatedKey());
        } while (queryRequest.getExclusiveStartKey() != null);
    }

    protected void createTable(String tableName) {
        DynamoDB dynamoDB = DynamoDbClientFactory.dynamoDb(dynamoDbConfig);
        try {
            List<KeySchemaElement> keySchemaElementList = Arrays.asList(new KeySchemaElement("artist", KeyType.HASH),
                    new KeySchemaElement("title", KeyType.RANGE));
            List<AttributeDefinition> attributeDefinitionList = Arrays.asList(new AttributeDefinition("artist", ScalarAttributeType.S),
                    new AttributeDefinition("title", ScalarAttributeType.S)
                    , new AttributeDefinition("fYear", ScalarAttributeType.N));
            GlobalSecondaryIndex fyearArtistIndex = new GlobalSecondaryIndex().withIndexName("fyear_artist_index")
                    .withProvisionedThroughput(new ProvisionedThroughput(10L, 10L))
                    .withKeySchema(new KeySchemaElement().withAttributeName("fYear").withKeyType(KeyType.HASH),
                            new KeySchemaElement().withAttributeName("artist").withKeyType(KeyType.RANGE))
                    .withProjection(
                            new Projection().withProjectionType("INCLUDE").withNonKeyAttributes("title"));

            GlobalSecondaryIndex artistFyearIndex = new GlobalSecondaryIndex().withIndexName("artist_fyear_index")
                    .withProvisionedThroughput(new ProvisionedThroughput(10L, 10L))
                    .withKeySchema(new KeySchemaElement().withAttributeName("artist").withKeyType(KeyType.HASH),
                            new KeySchemaElement().withAttributeName("fYear").withKeyType(KeyType.RANGE))
                    .withProjection(
                            new Projection().withProjectionType("INCLUDE").withNonKeyAttributes("title"));

            CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                    .withProvisionedThroughput(new ProvisionedThroughput(10L, 10L))
                    .withAttributeDefinitions(attributeDefinitionList).withKeySchema(keySchemaElementList)
                    .withGlobalSecondaryIndexes(fyearArtistIndex,artistFyearIndex);
            Table table = dynamoDB.createTable(createTableRequest);
            table.waitForActive();
            log.info("succeed to create table: %s.", tableName);
        } catch (Exception e) {
            log.error("Unable to create table: %s", e.getMessage());
        } finally {
            dynamoDB.shutdown();
        }
    }

    protected void dropTable(String tableName) {
        DynamoDB dynamoDB = DynamoDbClientFactory.dynamoDb(dynamoDbConfig);
        try {
            dynamoDB.getTable(tableName).delete();
            log.info("succeed to delete table: %s.", tableName);
        } catch (Exception e) {
            log.error("Unable to delete table: %s", e.getMessage());
        } finally {
            dynamoDB.shutdown();
        }
    }
}
