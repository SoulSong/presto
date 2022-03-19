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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * description :
 * 用来解析SQL中的查询条件，根据数据存储的特点将计算任务切分成多个split，交由coordinator调度到各个节点执行。
 *
 * @author songhaifeng
 * @date 2021/6/23 13:36
 */
public class DynamoDbSplitManager implements ConnectorSplitManager {
    private static final Logger LOGGER = Logger.get(DynamoDbSplitManager.class);

    private final String connectorId;
    private final DynamoDbClient dynamoDbClient;

    @Inject
    public DynamoDbSplitManager(DynamoDbConnectorId connectorId, DynamoDbClient dynamoDbClient) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.dynamoDbClient = requireNonNull(dynamoDbClient, "dynamoDbClient is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
                                          ConnectorSession session,
                                          ConnectorTableLayoutHandle layout,
                                          SplitSchedulingContext splitSchedulingContext) {
        DynamoDbTableLayoutHandle tableLayout = (DynamoDbTableLayoutHandle) layout;
        DynamoDbTableHandle tableHandle = tableLayout.getTable();

        // 获取condition
        TupleDomain<ColumnHandle> constraint = tableLayout.getTupleDomain();

        DynamoDbSplit split = new DynamoDbSplit(
                tableHandle.getConnectorId(),
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                constraint);

        List<DynamoDbSplit> splitList = ImmutableList.of(split);
        Collections.shuffle(splitList);

        return new FixedSplitSource(splitList);
    }

}
