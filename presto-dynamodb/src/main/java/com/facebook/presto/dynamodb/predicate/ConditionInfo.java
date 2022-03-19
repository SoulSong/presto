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
package com.facebook.presto.dynamodb.predicate;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;

import java.util.List;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/25 10:37
 */
public class ConditionInfo {
    /**
     * 使用originName
     */
    private String fieldName;
    private boolean equalOperator;
    private Object fieldValue;
    private List<Object> fieldValues;
    private DynamoDBMapperFieldModel.DynamoDBAttributeType dynamoDbAttributeType;
    private ComparisonOperator operator;

    public ConditionInfo(String fieldName, boolean equalOperator, Object fieldValue,
                         List<Object> fieldValues, DynamoDBMapperFieldModel.DynamoDBAttributeType dynamoDbAttributeType,
                         ComparisonOperator operator) {
        this.fieldName = fieldName;
        this.equalOperator = equalOperator;
        this.fieldValue = fieldValue;
        this.fieldValues = fieldValues;
        this.dynamoDbAttributeType = dynamoDbAttributeType;
        this.operator = operator;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public boolean getEqualOperator() {
        return equalOperator;
    }

    public void setEqualOperator(boolean equalOperator) {
        this.equalOperator = equalOperator;
    }

    public Object getFieldValue() {
        return fieldValue;
    }

    public void setFieldValue(Object fieldValue) {
        this.fieldValue = fieldValue;
    }

    public List<Object> getFieldValues() {
        return fieldValues;
    }

    public void setFieldValues(List<Object> fieldValues) {
        this.fieldValues = fieldValues;
    }

    public DynamoDBMapperFieldModel.DynamoDBAttributeType getDynamoDbAttributeType() {
        return dynamoDbAttributeType;
    }

    public void setDynamoDbAttributeType(DynamoDBMapperFieldModel.DynamoDBAttributeType dynamoDbAttributeType) {
        this.dynamoDbAttributeType = dynamoDbAttributeType;
    }

    public ComparisonOperator getOperator() {
        return operator;
    }

    public void setOperator(ComparisonOperator operator) {
        this.operator = operator;
    }
}
