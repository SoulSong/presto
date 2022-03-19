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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * description :
 * 组件元数据相关的操作，负责对外提供tableName、tableMetadata、columnNames、columnMetadata和schema相关的信息，确保presto能够识别并处理给定tableName.
 * 其实现委托为{@link DynamoDbClient}
 *
 * @author songhaifeng
 * @date 2021/6/22 16:30
 */
public class DynamoDbMetadata implements ConnectorMetadata {

    private final String connectorId;
    private final DynamoDbClient dynamoDbClient;

    @Inject
    public DynamoDbMetadata(DynamoDbConnectorId connectorId, DynamoDbClient dynamoDbClient) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.dynamoDbClient = requireNonNull(dynamoDbClient, "client is null");
    }

    /**
     * 对应show schemas命令
     *
     * @param session session
     * @return schemaNames
     */
    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return dynamoDbClient.getSchemaNames();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        DynamoDbTable table = dynamoDbClient.getTable(tableName);
        if (table == null) {
            return null;
        }

        return new DynamoDbTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

    /**
     * query 入口
     * @param session
     * @param table
     * @param constraint
     * @param desiredColumns
     * @return
     */
    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
        DynamoDbTableHandle handle = (DynamoDbTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new DynamoDbTableLayoutHandle(handle, constraint.getSummary(),desiredColumns));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {
        DynamoDbTableHandle dynamoDbTableHandle = (DynamoDbTableHandle) tableHandle;
        checkArgument(dynamoDbTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName schemaTableName = new SchemaTableName(dynamoDbTableHandle.getSchemaName(), dynamoDbTableHandle.getTableName());
        DynamoDbTable dynamoDbTable = dynamoDbClient.getTable(schemaTableName);
        return new ConnectorTableMetadata(schemaTableName, dynamoDbTable.getColumnsMetadata());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        DynamoDbTableHandle dynamoDbTableHandle = (DynamoDbTableHandle) tableHandle;
        checkArgument(dynamoDbTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName schemaTableName = new SchemaTableName(dynamoDbTableHandle.getSchemaName(), dynamoDbTableHandle.getTableName());
        DynamoDbTable table = dynamoDbClient.getTable(schemaTableName);
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;

        for (DynamoDbColumn column : table.getColumns()) {
            columnHandles.put(column.getLowerCaseName(), new DynamoDbColumnHandle(connectorId, column.getLowerCaseName(), column.getType(), index, column));
            index++;
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return ((DynamoDbColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        listTables(session, prefix).forEach(schemaTableName -> {
            columns.put(schemaTableName, dynamoDbClient.getTable(schemaTableName).getColumnsMetadata());
        });
        return columns.build();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix) {
        // 如果指定的schema为空，则获取所有schema/table实例
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
        List<String> schemaNames;
        if (schemaNameOrNull != null) {
            schemaNames = ImmutableList.of(schemaNameOrNull);
        } else {
            schemaNames = dynamoDbClient.getSchemaNames();
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : dynamoDbClient.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }
}
