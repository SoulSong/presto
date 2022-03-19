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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.facebook.presto.dynamodb.exception.NotSupportException;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/23 16:07
 */
public class DynamoDbRecordSetProvider implements ConnectorRecordSetProvider {
    private final String connectorId;
    private final DynamoDbClient dynamoDbClient;

    @Inject
    public DynamoDbRecordSetProvider(DynamoDbConnectorId connectorId, DynamoDbClient dynamoDbClient) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.dynamoDbClient = requireNonNull(dynamoDbClient, "dynamoDbClient is null");
    }

    /**
     * @param transactionHandle
     * @param session
     * @param split
     * @param columns           当前查询涉及到的所有列
     * @return
     */
    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
                                  ConnectorSplit split, List<? extends ColumnHandle> columns) {
        requireNonNull(split, "partitionChunk is null");
        DynamoDbSplit dynamoDbSplit = (DynamoDbSplit) split;
        checkArgument(dynamoDbSplit.getConnectorId().equals(connectorId), "split is not for this connector");

        ImmutableList.Builder<DynamoDbColumnHandle> handles = ImmutableList.builder();
        ImmutableSet.Builder<DynamoDbColumn> desiredColumns = ImmutableSet.builder();
        for (ColumnHandle handle : columns) {
            DynamoDbColumnHandle dynamoDbColumnHandle = (DynamoDbColumnHandle) handle;
            handles.add(dynamoDbColumnHandle);
            desiredColumns.add(dynamoDbColumnHandle.getDynamoDbColumn());
        }
        dynamoDbSplit.setDesiredColumns(desiredColumns.build());

        // 查询获取数据集
        Iterator<Map<String, AttributeValue>> tableData = dynamoDbClient.getTableData(dynamoDbSplit);

        return new DynamoDbRecordSet(dynamoDbSplit, handles.build(), tableData);
    }
}
