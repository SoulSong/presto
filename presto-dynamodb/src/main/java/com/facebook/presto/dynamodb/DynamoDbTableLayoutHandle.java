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
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * description :
 * 一张表的基本数据信息叫做TableLayout。一张表的TableLayout是在什么时候获取的呢？它是在对用户的 query 生成了执行计划树以后生成的。
 *
 * @author songhaifeng
 * @date 2021/6/23 13:53
 */
public class DynamoDbTableLayoutHandle implements ConnectorTableLayoutHandle {

    private final DynamoDbTableHandle table;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final Optional<Set<ColumnHandle>> desiredColumns;

    @JsonCreator
    public DynamoDbTableLayoutHandle(
            @JsonProperty("table") DynamoDbTableHandle table,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
            @JsonProperty("desiredColumns") Optional<Set<ColumnHandle>> desiredColumns) {
        this.table = requireNonNull(table, "table is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tuple is null");
        this.desiredColumns = requireNonNull(desiredColumns, "desiredColumns is null");
    }

    @JsonProperty
    public DynamoDbTableHandle getTable() {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain() {
        return tupleDomain;
    }

    @JsonProperty
    public Optional<Set<ColumnHandle>> getDesiredColumns() {
        return desiredColumns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DynamoDbTableLayoutHandle that = (DynamoDbTableLayoutHandle) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(tupleDomain, that.tupleDomain) &&
                Objects.equals(desiredColumns, that.desiredColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, tupleDomain, desiredColumns);
    }
}
