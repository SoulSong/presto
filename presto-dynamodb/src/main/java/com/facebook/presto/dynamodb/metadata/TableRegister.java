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
package com.facebook.presto.dynamodb.metadata;

import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.JsonType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.dynamodb.DynamoDbTable;

import java.util.Map;

public interface TableRegister {

    /**
     * primary index
     */
    String PRIMARY = "PRIMARY";

    void register(Map<String, DynamoDbTable> tableMap);

    /**
     * dynamoDBAttributeType转为prestoType
     * <pre>
     *     public static enum DynamoDBAttributeType {
     *         B,
     *         N,
     *         S,
     *         BS,
     *         NS,
     *         SS,
     *         BOOL,
     *         NULL,
     *         L,
     *         M;
     *     }
     * </pre>
     *
     * @param dynamoDbAttributeType dynamoDbAttributeType
     * @return {@link Type}
     */
    default Type dynamoDbAttributeTypeToPrestoType(String dynamoDbAttributeType) {
        switch (dynamoDbAttributeType) {
            case "S":
            case "N":
            case "B":
                // 由于dynamodb type无法很好的区分int、double、float等等，且其返回会以科学技术法显示，故考虑直接采用varchar类型
                // return DoubleType.DOUBLE;
                return VarcharType.VARCHAR;
            case "BOOL":
                return BooleanType.BOOLEAN;
            case "NS":
            case "L":
            case "M":
            case "SS":
            case "BS":
                return JsonType.JSON;
            default:
                throw new IllegalStateException("Illegal dynamodb attribute type: " + dynamoDbAttributeType);
        }
    }

}
