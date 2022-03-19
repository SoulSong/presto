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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static java.util.Objects.requireNonNull;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/23 13:28
 */
public class DynamoDbSplit implements ConnectorSplit {

    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private Set<DynamoDbColumn> desiredColumns;

    @JsonCreator
    public DynamoDbSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain) {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain() {
        return tupleDomain;
    }

    @JsonProperty
    public Set<DynamoDbColumn> getDesiredColumns() {
        return desiredColumns;
    }

    public void setDesiredColumns(Set<DynamoDbColumn> desiredColumns) {
         this.desiredColumns = desiredColumns;
    }

    /**
     * HARD_AFFINITY: Split 不能远程访问，必须在特定节点上
     * <p>
     * SOFT_AFFINITY：连接器拆分提供了可供引擎选择的首选节点列表，但不是强制性的。
     * <p>
     * NO_PREFERENCE：Split 可以远程访问，并且可以在任何节点上
     *
     * @return {@link NodeSelectionStrategy}
     */
    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy() {
        return NO_PREFERENCE;
    }

    /**
     * 空列表表示没有偏好
     *
     * @param sortedCandidates 候选优先节点
     * @return HostAddress
     */
    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates) {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo() {
        return this;
    }
}
