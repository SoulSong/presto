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

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Strings;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.dynamodb.DynamoDbClient.OBJECT_MAPPER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * description :
 * 定义应该如何根据拿到的一个split到库中查询数据。
 *
 * @author songhaifeng
 * @date 2021/6/23 15:46
 */
public class DynamoDbRecordCursor implements RecordCursor {

    private final List<DynamoDbColumnHandle> columnHandles;
    private final String[] fieldToOriginName;
    private Map<String, AttributeValue> row;
    private final Iterator<Map<String, AttributeValue>> tableData;

    public DynamoDbRecordCursor(List<DynamoDbColumnHandle> columnHandles,
                                Iterator<Map<String, AttributeValue>> tableData) {
        this.columnHandles = columnHandles;
        this.tableData = tableData;

        // 获取指定索引列对应的原始字段名，用于从map中根据originName取数
        fieldToOriginName = new String[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            fieldToOriginName[i] = columnHandles.get(i).getDynamoDbColumn().getOriginName();
        }
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    public DynamoDBMapperFieldModel.DynamoDBAttributeType getDynamoDBAttributeType(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getDynamoDbColumn().getDynamoDbAttributeType();
    }

    @Override
    public boolean advanceNextPosition() {
        if (!tableData.hasNext()) {
            return false;
        }
        row = tableData.next();
        return true;
    }

    /**
     * 需要根据字段的类型针对性处理，特别是复杂类型和集合类型均转换为json字符串处理
     *
     * @param field field
     * @return String
     */
    private String getFieldValue(int field) {
        checkState(row != null, "Cursor has not been advanced yet");

        String originName = fieldToOriginName[field];
        DynamoDBMapperFieldModel.DynamoDBAttributeType type = getDynamoDBAttributeType(field);
        switch (type) {
            case S:
                return row.get(originName) == null ? null : row.get(originName).getS();
            case N:
                if (row.get(originName) == null) {
                    return null;
                } else {
                    Map<String, Object> mapValue = ItemUtils.toSimpleMapValue(row);
                    // todo 兼容dynamodb中NULL类型，目前方案不是很优雅
                    if(null == mapValue.get(originName)){
                        return null;
                    }

                    return mapValue.get(originName).toString();
                }
            case L:
            case NS:
            case M:
            case SS:
                if (row.get(originName) == null) {
                    return null;
                } else {
                    Map<String, Object> mapValue = ItemUtils.toSimpleMapValue(row);
                    try {
                        return OBJECT_MAPPER.writeValueAsString(mapValue.get(originName));
                    } catch (JsonProcessingException e) {
                        throw new IllegalArgumentException(String.format("Convert field[%s] value[%s]to json failed.", originName, mapValue.get(originName)));
                    }
                }
            case B:
                if (row.get(originName) == null) {
                    return null;
                } else {
                    ByteBuffer buffer = row.get(originName).getB();
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    try {
                        IOUtils.copy(new GZIPInputStream(new ByteArrayInputStream(buffer.array())), out);
                        return out.toString();
                    } catch (IOException e) {
                        throw new IllegalArgumentException(String.format("Gzip uncompress error, message : %s", e.getMessage()));
                    }
                }
            case BOOL:
                return row.get(originName) == null ? null : row.get(originName).getN();
            default:
                throw new NotSupportedException(String.format("Not support for field [%s] with type [%s]  ", originName, type.name()));
        }
    }

    @Override
    public boolean getBoolean(int field) {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field) {
        checkFieldType(field, BIGINT);
        return Long.parseLong(getFieldValue(field));
    }

    @Override
    public double getDouble(int field) {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field) {
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public Object getObject(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, Type expected) {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close() {
    }
}
