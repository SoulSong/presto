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
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/23 14:19
 */
public class DynamoDbColumn {
    private final String lowerCaseName;
    private final String originName;
    private final Type type;
    private final DynamoDBMapperFieldModel.DynamoDBAttributeType dynamoDbAttributeType;

    @JsonCreator
    public DynamoDbColumn(
            @JsonProperty("lowerCaseName") String lowerCaseName,
            @JsonProperty("originName") String originName,
            @JsonProperty("type") Type type,
            @JsonProperty("dynamoDbAttributeType") DynamoDBMapperFieldModel.DynamoDBAttributeType dynamoDbAttributeType) {
        checkArgument(!isNullOrEmpty(lowerCaseName), "name is null or is empty");
        this.lowerCaseName = lowerCaseName;
        this.originName = originName;
        this.type = requireNonNull(type, "type is null");
        this.dynamoDbAttributeType = requireNonNull(dynamoDbAttributeType, "dynamoDbAttributeType is null");
    }

    @JsonProperty
    public String getLowerCaseName() {
        return lowerCaseName;
    }

    @JsonProperty
    public String getOriginName() {
        return originName;
    }

    @JsonProperty
    public Type getType() {
        return type;
    }

    @JsonProperty
    public DynamoDBMapperFieldModel.DynamoDBAttributeType getDynamoDbAttributeType() {
        return dynamoDbAttributeType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lowerCaseName, originName, type, dynamoDbAttributeType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        DynamoDbColumn other = (DynamoDbColumn) obj;
        return Objects.equals(this.lowerCaseName, other.lowerCaseName) &&
                Objects.equals(this.originName, other.originName) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.dynamoDbAttributeType, other.dynamoDbAttributeType);
    }

    @Override
    public String toString() {
        return lowerCaseName + ":" + originName + ":" + type + ":" + dynamoDbAttributeType.name();
    }


}
