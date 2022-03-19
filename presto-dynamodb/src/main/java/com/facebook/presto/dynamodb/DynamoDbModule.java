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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

import javax.inject.Inject;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.util.Objects.requireNonNull;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/22 15:55
 */
public class DynamoDbModule implements Module {
    private final String connectorId;
    private final TypeManager typeManager;

    public DynamoDbModule(String connectorId, TypeManager typeManager) {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(DynamoDbConnector.class).in(Scopes.SINGLETON);
        binder.bind(DynamoDbConnectorId.class).toInstance(new DynamoDbConnectorId(connectorId));
        binder.bind(DynamoDbMetadata.class).in(Scopes.SINGLETON);
        binder.bind(DynamoDbClient.class).in(Scopes.SINGLETON);
        binder.bind(DynamoDbSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(DynamoDbRecordSetProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(DynamoDbConfig.class);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(DynamoDbTable.class));
    }

    @Singleton
    @Provides
    public static AmazonDynamoDB dynamoDbClient(DynamoDbConfig dynamoDbConfig) {
        return DynamoDbClientFactory.dynamoDbClient(dynamoDbConfig);
    }

    @Singleton
    @Provides
    public static DynamoDBMapper dynamoDbMapper(AmazonDynamoDB dynamoDbClient) {
        return DynamoDbClientFactory.dynamoDbMapper(dynamoDbClient);
    }

    @Singleton
    @Provides
    public static DynamoDB dynamoDb(AmazonDynamoDB dynamoDbClient) {
        return DynamoDbClientFactory.dynamoDb(dynamoDbClient);
    }


    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            return typeManager.getType(parseTypeSignature(value));
        }
    }
}
