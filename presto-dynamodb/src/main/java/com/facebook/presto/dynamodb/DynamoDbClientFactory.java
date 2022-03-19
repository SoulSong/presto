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

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import org.apache.commons.lang.StringUtils;

/**
 * @author songhaifeng
 */
public class DynamoDbClientFactory {

    public static AmazonDynamoDB dynamoDbClient(DynamoDbConfig dynamoDbConfig) {
        AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(dynamoDbConfig.getCredentials());
        if (StringUtils.isNotEmpty(dynamoDbConfig.getEndpoint())) {
            builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(dynamoDbConfig.getEndpoint(), dynamoDbConfig.getRegion()));
        } else {
            builder.withRegion(dynamoDbConfig.getRegion());
        }
        return builder.build();
    }

    public static DynamoDBMapper dynamoDbMapper(DynamoDbConfig dynamoDbConfig) {
        return new DynamoDBMapper(dynamoDbClient(dynamoDbConfig));
    }

    public static DynamoDBMapper dynamoDbMapper(AmazonDynamoDB dynamoDb) {
        return new DynamoDBMapper(dynamoDb);
    }

    public static DynamoDB dynamoDb(DynamoDbConfig dynamoDbConfig) {
        return new DynamoDB(dynamoDbClient(dynamoDbConfig));
    }

    public static DynamoDB dynamoDb(AmazonDynamoDB dynamoDb) {
        return new DynamoDB(dynamoDb);
    }
}
