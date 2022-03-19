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
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.dynamodb.DynamoDbColumn;
import com.facebook.presto.dynamodb.DynamoDbColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * description :
 * 当前支持多字段的 =、!=、>、>=、<、<=、 is null、is not null、in操作符，所有filter下推采用and连接符。
 * 其中所有数据类型均支持null判断，其他比对操作符仅支持N和S类型的filter下推。
 *
 * @author songhaifeng
 * @date 2021/6/25 10:48
 */
public class PredicateParser {
    private static final Logger LOGGER = Logger.get(PredicateParser.class);

    /**
     * query特性举例：
     * title = 'aa' and title = 'bb' ： 此类查询不会被正常执行，在前置执行计划解析后即被拦截返回；
     *
     * @param domainMap domainMap
     * @return ConditionInfos
     */
    public static List<ConditionInfo> translateConstraintToCondition(Map<ColumnHandle, Domain> domainMap) {
        List<ConditionInfo> conditionInfoList = new ArrayList<>();
        for (Map.Entry<ColumnHandle, Domain> entry : domainMap.entrySet()) {
            conditionInfoList.addAll(buildConditionInfo(entry));
        }
        return conditionInfoList;
    }

    /**
     * 目前仅支持所有的filter连接符为and
     *
     * @param entry entry
     * @return ConditionInfo
     */
    private static List<ConditionInfo> buildConditionInfo(Map.Entry<ColumnHandle, Domain> entry) {
        DynamoDbColumnHandle columnHandle = (DynamoDbColumnHandle) entry.getKey();
        DynamoDbColumn column = columnHandle.getDynamoDbColumn();
        Domain domain = entry.getValue();
        String fieldName = column.getOriginName();
        DynamoDBMapperFieldModel.DynamoDBAttributeType dynamoDbAttributeType = column.getDynamoDbAttributeType();

        if (domain.getValues().isNone() && domain.isNullAllowed()) {
            return Collections.singletonList(new ConditionInfo(fieldName, false, null, null,
                    dynamoDbAttributeType, ComparisonOperator.NULL));
        }
        if (domain.getValues().isAll() && !domain.isNullAllowed()) {
            return Collections.singletonList(new ConditionInfo(fieldName, false, null, null,
                    dynamoDbAttributeType, ComparisonOperator.NOT_NULL));
        }

        List<ConditionInfo> conditionInfos = new ArrayList<>();

        // 排除上面的null判断，后续仅支持N和S类型的filter下推
        if (DynamoDBMapperFieldModel.DynamoDBAttributeType.S.equals(dynamoDbAttributeType)
                || DynamoDBMapperFieldModel.DynamoDBAttributeType.N.equals(dynamoDbAttributeType)) {
            List<Object> singleValues = new ArrayList<>();
            ConditionInfo lastConditionInfo = null;
            for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                // 处理sql文本中的等值比对
                if (range.isSingleValue()) {
                    singleValues.add(translateValue(range.getSingleValue()));
                } else {
                    // 不仅处理>、>=，还处理!=
                    if (!range.isLowUnbounded()) {
                        ConditionInfo currentConditionInfo = new ConditionInfo(fieldName, false,
                                translateValue(range.getLowBoundedValue()), null, dynamoDbAttributeType,
                                range.isLowInclusive() ? ComparisonOperator.GE : ComparisonOperator.GT);
                        // 将当前值上一个info比对，如果其除了operator不一致，且为GT和LT对应，其他均相同，则说明是!=操作符。
                        // 故需要替换最后一个conditionInfo的operator为not_eq
                        if (lastConditionInfo != null && lastConditionInfo.getFieldName().equals(currentConditionInfo.getFieldName())
                                && lastConditionInfo.getFieldValue().equals(currentConditionInfo.getFieldValue())
                                && lastConditionInfo.getOperator().equals(ComparisonOperator.LT)
                                && currentConditionInfo.getOperator().equals(ComparisonOperator.GT)) {
                            lastConditionInfo.setOperator(ComparisonOperator.NE);
                        } else {
                            conditionInfos.add(currentConditionInfo);
                        }
                    }
                    // 不仅处理<、<=，还处理!=，在!=场景下，其先走，可记录lastConditionInfo便于比对
                    if (!range.isHighUnbounded()) {
                        lastConditionInfo = new ConditionInfo(fieldName, false,
                                translateValue(range.getHighBoundedValue()), null, dynamoDbAttributeType,
                                range.isHighInclusive() ? ComparisonOperator.LE : ComparisonOperator.LT);
                        conditionInfos.add(lastConditionInfo);
                    }
                }
            }
            // 处理单个字段的等值查询
            if (singleValues.size() == 1) {
                return Collections.singletonList(new ConditionInfo(fieldName, true, singleValues.get(0),
                        null, dynamoDbAttributeType, ComparisonOperator.EQ));
            } else if (singleValues.size() > 1) {
                // 处理类似 title = 'aa' or title = 'bb'
                return Collections.singletonList(new ConditionInfo(fieldName, false, null,
                        singleValues, dynamoDbAttributeType, ComparisonOperator.IN));
            }
        }
        return conditionInfos;
    }

    /**
     * 获取value值
     *
     * @param source source
     * @return retValue
     */
    private static Object translateValue(Object source) {
        if (source instanceof Slice) {
            return ((Slice) source).toStringUtf8();
        }
        return source;
    }

}
