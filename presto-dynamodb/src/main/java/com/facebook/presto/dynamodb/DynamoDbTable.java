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

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/23 17:32
 */
public class DynamoDbTable {
    private String schemaName;
    private String lowerCaseTableName;
    private String originTableName;
    private Map<String, DynamoDbColumn> columnMap;
    private List<DynamoDbColumn> columns;
    private List<ColumnMetadata> columnsMetadata;
    private String hashKeyName;
    private String rangeKeyName;
    private Map<String,DynamoDbIndex> dynamoDbIndexMap;

    @JsonCreator
    public DynamoDbTable(@JsonProperty("schemaName") String schemaName,
                         @JsonProperty("lowerCaseTableName") String lowerCaseTableName,
                         @JsonProperty("tableName") String originTableName,
                         @JsonProperty("columnMap") Map<String, DynamoDbColumn> columnMap,
                         @JsonProperty("hashKeyName") String hashKeyName,
                         @JsonProperty("rangeKeyName") String rangeKeyName,
                         @JsonProperty("dynamoDbIndexMap") Map<String,DynamoDbIndex> dynamoDbIndexMap) {
        checkArgument(!isNullOrEmpty(lowerCaseTableName), "lowerCaseTableName is null or is empty");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.lowerCaseTableName = requireNonNull(lowerCaseTableName, "lowerCaseTableName is null");
        this.originTableName = requireNonNull(originTableName, "originTableName is null");
        this.columnMap = requireNonNull(columnMap, "columnMap is null");
        this.columns = new ArrayList<>(columnMap.values());
        this.columnsMetadata = columns.stream()
                .map(column -> new ColumnMetadata(column.getLowerCaseName(), column.getType()))
                .collect(Collectors.toList());
        this.hashKeyName = requireNonNull(hashKeyName, "hashKeyName is null");
        this.rangeKeyName = rangeKeyName;
        this.dynamoDbIndexMap = dynamoDbIndexMap;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getLowerCaseTableName() {
        return lowerCaseTableName;
    }

    @JsonProperty
    public String getOriginTableName() {
        return originTableName;
    }

    @JsonProperty
    public List<DynamoDbColumn> getColumns() {
        return columns;
    }

    @JsonProperty
    public List<ColumnMetadata> getColumnsMetadata() {
        return columnsMetadata;
    }

    @JsonProperty
    public Map<String, DynamoDbColumn> getColumnMap() {
        return columnMap;
    }

    @JsonProperty
    public String getHashKeyName() {
        return hashKeyName;
    }

    @JsonProperty
    public String getRangeKeyName() {
        return rangeKeyName;
    }

    @JsonProperty
    public Map<String, DynamoDbIndex> getDynamoDbIndexMap() {
        return dynamoDbIndexMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DynamoDbTable that = (DynamoDbTable) o;
        return Objects.equals(schemaName, that.schemaName) && Objects.equals(lowerCaseTableName, that.lowerCaseTableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaName, lowerCaseTableName);
    }
}
