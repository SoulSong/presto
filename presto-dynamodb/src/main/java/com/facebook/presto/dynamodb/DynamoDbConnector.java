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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.inject.Inject;

import static java.util.Objects.requireNonNull;

/**
 * description :
 * 允许 Presto 获取对连接器提供的各种服务的引用。Connector实例由ConnectorFactory实例创建。
 *
 * @author songhaifeng
 * @date 2021/6/22 15:54
 */

public class DynamoDbConnector implements Connector {
    private static final Logger LOGGER = Logger.get(DynamoDbConnector.class);

    private final DynamoDbClient dynamoDbClient;
    private final DynamoDbMetadata metadata;
    private final DynamoDbSplitManager splitManager;
    private final DynamoDbRecordSetProvider recordSetProvider;

    @Inject
    public DynamoDbConnector(
            DynamoDbClient dynamoDbClient,
            DynamoDbMetadata metadata,
            DynamoDbSplitManager splitManager,
            DynamoDbRecordSetProvider recordSetProvider) {
        this.dynamoDbClient = requireNonNull(dynamoDbClient, "dynamoDbClient is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        return DynamoDbTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return recordSetProvider;
    }

    @Override
    public void shutdown() {
        try {
            dynamoDbClient.shutdown();
        } catch (Exception e) {
            LOGGER.error(e, "Error shutting down connector");
        }
    }
}
