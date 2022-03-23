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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.dynamodb.exception.NotSupportException;
import com.facebook.presto.dynamodb.metadata.LoadByHttpTableRegister;
import com.facebook.presto.dynamodb.metadata.LoadByJarTableRegister;
import com.facebook.presto.dynamodb.metadata.TableRegister;
import com.facebook.presto.dynamodb.predicate.ConditionInfo;
import com.facebook.presto.dynamodb.predicate.PredicateParser;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.TableNotFoundException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections.CollectionUtils;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.facebook.presto.dynamodb.DynamoDbConfig.HTTP;
import static com.facebook.presto.dynamodb.DynamoDbConfig.JAR;
import static com.facebook.presto.dynamodb.metadata.TableRegister.PRIMARY;
import static java.util.Objects.requireNonNull;

/**
 * description :
 * 所有tableName、fieldName均存储为小写，原始名称存放在originName中。除最终构造queryExpression外，其余场景均
 *
 * @author songhaifeng
 * @date 2021/6/22 17:02
 */
public class DynamoDbClient {
    private static final Logger log = Logger.get(DynamoDbClient.class);

    private final DynamoDbConfig dynamoDbConfig;
    private final AmazonDynamoDBClient dynamoDbClient;
    private final DynamoDB dynamoDb;

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String DEFAULT_SCHEMA = "default";
    private static final String NONE_INDEX = "NONE_INDEX";
    private static final String CONDITIONAL_OPERATOR = " AND ";
    private static final String EQ = " = ";
    private static final String NE = " <> ";
    private static final String GT = " > ";
    private static final String GE = " >= ";
    private static final String LT = " < ";
    private static final String LE = " <= ";
    private static final String IN = " in ";
    private static final String LABEL_PREFIX = ":";
    private static final CharSequence IN_DELIMITER = ",";
    private static final CharSequence IN_PREFIX = "(";
    private static final CharSequence IN_SUFFIX = ")";
    private static final String POUND_SIGN = "#";

    /**
     * key：tableName
     * value： {@link DynamoDbTable}
     */
    private final Map<String, DynamoDbTable> tableMap = new ConcurrentHashMap<>();

    @Inject
    public DynamoDbClient(DynamoDbConfig dynamoDbConfig, AmazonDynamoDB dynamoDbClient, DynamoDBMapper dynamoDbMapper, DynamoDB dynamoDb) throws IOException {
        this.dynamoDbConfig = requireNonNull(dynamoDbConfig, "dynamoDbConfig is null");
        this.dynamoDbClient = (AmazonDynamoDBClient) requireNonNull(dynamoDbClient, "dynamoDbClient is null");
        this.dynamoDb = requireNonNull(dynamoDb, "dynamoDb is null");

        TableRegister tableRegister = null;
        log.info("current schema register type is %s", dynamoDbConfig.getSchemaRegisterType().toLowerCase(Locale.ENGLISH));
        switch (dynamoDbConfig.getSchemaRegisterType().toLowerCase(Locale.ENGLISH)) {
            case JAR:
                tableRegister = new LoadByJarTableRegister(dynamoDbConfig,
                        requireNonNull(dynamoDbMapper, "dynamoDbMapper is null"), dynamoDb, getSchemaNames());
                break;
            case HTTP:
                tableRegister = new LoadByHttpTableRegister(dynamoDbConfig, getSchemaNames());
                break;
            default:
                log.warn("Can not match any tableRegister. please check your config.");
        }
        if (tableRegister != null) {
            tableRegister.register(tableMap);
        }
    }

    /**
     * 默认使用Default即可
     *
     * @return schemaNames
     */
    public List<String> getSchemaNames() {
        return ImmutableList.of(DEFAULT_SCHEMA);
    }

    /**
     * 获取指定schema下的所有tableNames
     *
     * @param schema 指定schema
     * @return tableNames
     */
    public Set<String> getTableNames(String schema) {
        checkSchema(schema);
        return ImmutableSet.copyOf(tableMap.keySet());
    }

    /**
     * 获取表信息
     *
     * @param schemaTableName schema & table
     * @return {@link DynamoDbTable}
     */
    public DynamoDbTable getTable(SchemaTableName schemaTableName) {
        checkSchema(schemaTableName.getSchemaName());
        DynamoDbTable table = tableMap.get(schemaTableName.getTableName());
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }
        return table;
    }

    /**
     * 根据提交的sql文本获取原始数据，支持project和filter下推
     *
     * @param split {@link DynamoDbSplit}
     * @return 数据集迭代器
     */
    public Iterator<Map<String, AttributeValue>> getTableData(DynamoDbSplit split) {
        Optional<Map<ColumnHandle, Domain>> domains = split.getTupleDomain().getDomains();
        if (domains.isPresent()) {
            List<ConditionInfo> conditionInfos = PredicateParser.translateConstraintToCondition(domains.get());
            String schemaName = split.getSchemaName();
            String tableName = split.getTableName();
            Set<DynamoDbColumn> desiredColumns = split.getDesiredColumns();
            DynamoDbTable table = tableMap.get(tableName);

            if (CollectionUtils.isEmpty(conditionInfos)) {
                ScanRequest scanRequest = buildScanRequest(table, desiredColumns);
                return executeScan(scanRequest).iterator();
            }

            QueryRequest queryRequest = buildQueryRequest(table, conditionInfos, desiredColumns);
            return executeQuery(queryRequest).iterator();

        }

        throw new NotSupportException("Not support scan table.");
    }

    private ScanRequest buildScanRequest(DynamoDbTable table, Set<DynamoDbColumn> desiredColumns) {
        // 构建projection
        Set<String> projections = desiredColumns.stream().map(DynamoDbColumn::getOriginName).collect(Collectors.toSet());
        // 构建QueryRequest
        String originTableName = table.getOriginTableName();
        ScanRequest scanRequest = new ScanRequest();
        scanRequest.withTableName(originTableName)
                .withLimit(dynamoDbConfig.getLimit());

        // 主要处理count(*)场景下projections为empty
        if (projections.size() > 0) {
            Map<String, String> expressionAttributeNames = new HashMap<>(projections.size());
            projections.forEach(projection -> expressionAttributeNames.put(POUND_SIGN.concat(projection.toLowerCase(Locale.ENGLISH)), projection));
            scanRequest.withExpressionAttributeNames(expressionAttributeNames)
                    .withProjectionExpression(projections.stream()
                            .map(projection -> POUND_SIGN.concat(projection.toLowerCase(Locale.ENGLISH)))
                            .collect(Collectors.joining(IN_DELIMITER)));
        }
        return scanRequest;
    }


    /**
     * 根据查询语句中的filter和projection信息构造dynamodb query request
     *
     * @param table             tableName,必须是原始表明，大小写敏感
     * @param allConditionInfos 所有解析出的condition
     * @param desiredColumns    待投影字段
     * @return {@link QueryRequest}
     */
    private QueryRequest buildQueryRequest(DynamoDbTable table, List<ConditionInfo> allConditionInfos, Set<DynamoDbColumn> desiredColumns) {
        // hashKey仅支持等值比对
        Set<String> hashKeyCandidateFieldSet = allConditionInfos.stream().filter(ConditionInfo::getEqualOperator).map(ConditionInfo::getFieldName).collect(Collectors.toSet());

        // 如果等值比对集合为空则提示异常，当前仅支持根据hashKey的query操作
        if (CollectionUtils.isEmpty(hashKeyCandidateFieldSet)) {
            throw new NotSupportException("Not support scan table.");
        }

        // rangeKey其仅支持=、<、<=、>、>=操作符,且仅在全局过滤项中唯一出现;
        Set<String> rangeKeyCandidateFieldSet = allConditionInfos.stream().filter(conditionInfo -> ComparisonOperator.EQ.equals(conditionInfo.getOperator()) || ComparisonOperator.GT.equals(conditionInfo.getOperator()) || ComparisonOperator.GE.equals(conditionInfo.getOperator()) || ComparisonOperator.LT.equals(conditionInfo.getOperator()) || ComparisonOperator.LE.equals(conditionInfo.getOperator())).collect(Collectors.groupingBy(ConditionInfo::getFieldName, Collectors.counting())).entrySet().stream().filter(entry -> entry.getValue() == 1).map(Map.Entry::getKey).collect(Collectors.toSet());
        // 记录hashKey和rangeKey候选者
        Map<String, ConditionInfo> allSingleConditionInfoMap = allConditionInfos.stream().filter(conditionInfo -> hashKeyCandidateFieldSet.contains(conditionInfo.getFieldName()) || rangeKeyCandidateFieldSet.contains(conditionInfo.getFieldName())).collect(Collectors.toMap(ConditionInfo::getFieldName, info -> info));

        // 构建projection
        Set<String> projections = desiredColumns.stream().map(DynamoDbColumn::getOriginName).collect(Collectors.toSet());
        Map<String, String> expressionAttributeNames = new HashMap<>(projections.size());
        projections.forEach(projection -> expressionAttributeNames.put(POUND_SIGN.concat(projection.toLowerCase(Locale.ENGLISH)), projection));

        // 构建QueryRequest
        String originTableName = table.getOriginTableName();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.withTableName(originTableName)
                .withLimit(dynamoDbConfig.getLimit())
                .withProjectionExpression(projections.stream()
                        .map(projection -> POUND_SIGN.concat(projection.toLowerCase(Locale.ENGLISH)))
                        .collect(Collectors.joining(IN_DELIMITER)))
                .withExpressionAttributeNames(expressionAttributeNames);

        // 根据hashKey&rangeKey比对项找到最佳的查询入口
        String indexName = findBestMatchIndex(table, hashKeyCandidateFieldSet, rangeKeyCandidateFieldSet, allSingleConditionInfoMap, projections);
        if (NONE_INDEX.equals(indexName)) {
            throw new NotSupportException("No suitable hashKey filter item in the current sql.");
        }
        log.info("query table : %s , index : %s", originTableName, indexName);
        queryRequest.withIndexName(PRIMARY.equals(indexName) ? null : indexName);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        queryRequest.withExpressionAttributeValues(expressionAttributeValues);

        //获取当前的hashKey和rangeKey
        DynamoDbIndex bestMatchIndex = table.getDynamoDbIndexMap().get(indexName);
        String hashKey = bestMatchIndex.getHashKey();
        String rangeKey = bestMatchIndex.getRangeKey();
        // 构造keyConditionExpression
        List<String> keyConditionExpressions = new ArrayList<>(2);
        keyConditionExpressions.add(buildExpressionByConditionInfo(allSingleConditionInfoMap.get(hashKey), expressionAttributeValues));
        allConditionInfos.remove(allSingleConditionInfoMap.get(hashKey));
        if (rangeKey != null && allSingleConditionInfoMap.containsKey(rangeKey)) {
            keyConditionExpressions.add(buildExpressionByConditionInfo(allSingleConditionInfoMap.get(rangeKey), expressionAttributeValues));
            allConditionInfos.remove(allSingleConditionInfoMap.get(rangeKey));
        }
        String keyConditionExpression = String.join(CONDITIONAL_OPERATOR, keyConditionExpressions);
        log.info("keyConditionExpression : %s", keyConditionExpression);
        queryRequest.withKeyConditionExpression(keyConditionExpression);
        // 去除keyCondition剩下的可组合成filterExpression下推
        if (allConditionInfos.size() > 0) {
            List<String> filterConditionExpressions = new ArrayList<>(allConditionInfos.size());
            allConditionInfos.forEach(conditionInfo -> {
                // rangeKey不支持出现在filterExpression中,无法下推
                if (conditionInfo.getFieldName().equals(rangeKey)) {
                    return;
                }
                filterConditionExpressions.add(buildExpressionByConditionInfo(conditionInfo, expressionAttributeValues));
            });
            if (filterConditionExpressions.size() > 0) {
                String filterConditionExpression = String.join(CONDITIONAL_OPERATOR, filterConditionExpressions);
                queryRequest.withFilterExpression(filterConditionExpression);
            }
        }
        return queryRequest;
    }

    /**
     * 根据hashKey&rangeKey候选过滤项与投影字段，遍历index集合获取最佳的查询对象。
     * 打分机制：globalFactor表示全局系数，默认值1。score= globalFactor*(score_hashKey+score_projections+score_rangeKey)
     * 1、hashKey:存在于等值过滤项集合则得10分；不存在于等值过滤项集合，则globalFactor为0；
     * 2、projections:完全一致得4分；全包含得3分；部分缺失则globalFactor为0；
     * 3、rangeKey:存在于候选过滤项集合且为等值比对则得7分；存在于候选过滤项集合且为非等值比对则得5分；不存在符合条件的rangeKey则得0分；
     *
     * @param table                     待查询表
     * @param hashKeyCandidateFieldSet  hashKey等值过滤项filedName集合
     * @param rangeKeyCandidateFieldSet rangeKey候选过滤项filedName集合
     * @param allSingleConditionInfoMap hashKey&rangeKey过滤项
     * @param projections               投影字段列表
     * @return 最高分indexName；如果不存在得分大于0的候选者，则返回 NONE_INDEX
     */
    private String findBestMatchIndex(DynamoDbTable table, Set<String> hashKeyCandidateFieldSet, Set<String> rangeKeyCandidateFieldSet, Map<String, ConditionInfo> allSingleConditionInfoMap, Set<String> projections) {
        final Map<String, Integer> candidates = new HashMap<>();
        table.getDynamoDbIndexMap().values().forEach(dynamoDbIndex -> {
            String indexName = dynamoDbIndex.getIndexName();
            // score = globalFactor * subScore
            int globalFactor = 1;
            int subScore = 0;
            // hashKey
            if (hashKeyCandidateFieldSet.contains(dynamoDbIndex.getHashKey())) {
                subScore += 10;
            } else {
                globalFactor = 0;
                candidates.put(indexName, globalFactor * subScore);
                return;
            }

            // projections
            if (dynamoDbIndex.getProjections().containsAll(projections)) {
                if (dynamoDbIndex.getProjections().size() == projections.size()) {
                    subScore += 4;
                } else {
                    subScore += 3;
                }
            } else {
                globalFactor = 0;
                candidates.put(indexName, globalFactor * subScore);
                return;
            }

            // rangeKey
            if (rangeKeyCandidateFieldSet.contains(dynamoDbIndex.getRangeKey())) {
                if (ComparisonOperator.EQ.equals(allSingleConditionInfoMap.get(dynamoDbIndex.getRangeKey()).getOperator())) {
                    subScore += 7;
                } else {
                    subScore += 5;
                }
            }
            candidates.put(indexName, globalFactor * subScore);
        });
        // 对候选者进行排序
        List<Map.Entry<String, Integer>> list = candidates.entrySet().stream().filter(entry -> entry.getValue() > 0)
                .sorted(Comparator.comparing(entry -> ((Map.Entry<String, Integer>) entry).getValue()).reversed()).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(list)) {
            // 获取最高分
            return list.get(0).getKey();
        }

        return NONE_INDEX;
    }

    /**
     * 根据condition构造expression，同时赋值expressionValues
     *
     * @param conditionInfo             conditionInfo
     * @param expressionAttributeValues expressionAttributeValues
     * @return expression
     */
    private String buildExpressionByConditionInfo(ConditionInfo conditionInfo, final Map<String, AttributeValue> expressionAttributeValues) {
        String valueLab;
        String poundSignFieldName = POUND_SIGN.concat(conditionInfo.getFieldName().toLowerCase(Locale.ENGLISH));
        switch (conditionInfo.getOperator()) {
            case NULL:
                return "attribute_not_exists(" + poundSignFieldName + ")";
            case NOT_NULL:
                return "attribute_exists(" + poundSignFieldName + ")";
            case IN:
                List<Object> values = conditionInfo.getFieldValues();
                List<String> labels = new ArrayList<>(values.size());
                for (int index = 0; index < values.size(); index++) {
                    String label = LABEL_PREFIX + conditionInfo.getFieldName() + conditionInfo.getOperator().name() + index;
                    labels.add(label);
                    appendExpressionAttributeValues(label, conditionInfo, values.get(index), expressionAttributeValues);
                }
                return poundSignFieldName + IN + labels.stream().collect(Collectors.joining(IN_DELIMITER, IN_PREFIX, IN_SUFFIX));
            case EQ:
                valueLab = buildValueLab(conditionInfo);
                appendExpressionAttributeValues(valueLab, conditionInfo, conditionInfo.getFieldValue(), expressionAttributeValues);
                return poundSignFieldName + EQ + valueLab;
            case NE:
                valueLab = buildValueLab(conditionInfo);
                appendExpressionAttributeValues(valueLab, conditionInfo, conditionInfo.getFieldValue(), expressionAttributeValues);
                return poundSignFieldName + NE + valueLab;
            case GT:
                valueLab = buildValueLab(conditionInfo);
                appendExpressionAttributeValues(valueLab, conditionInfo, conditionInfo.getFieldValue(), expressionAttributeValues);
                return poundSignFieldName + GT + valueLab;
            case GE:
                valueLab = buildValueLab(conditionInfo);
                appendExpressionAttributeValues(valueLab, conditionInfo, conditionInfo.getFieldValue(), expressionAttributeValues);
                return poundSignFieldName + GE + valueLab;
            case LE:
                valueLab = buildValueLab(conditionInfo);
                appendExpressionAttributeValues(valueLab, conditionInfo, conditionInfo.getFieldValue(), expressionAttributeValues);
                return poundSignFieldName + LE + valueLab;
            case LT:
                valueLab = buildValueLab(conditionInfo);
                appendExpressionAttributeValues(valueLab, conditionInfo, conditionInfo.getFieldValue(), expressionAttributeValues);
                return poundSignFieldName + LT + valueLab;
            default:
                // TODO 添加更多的类型处理
                throw new IllegalArgumentException(String.format("Not support %s operator in the filter expression.", conditionInfo.getOperator().name()));
        }
    }

    /**
     * 根据conditionInfo构造expressionValue的key标签
     *
     * @param conditionInfo conditionInfo
     * @return key
     */
    private String buildValueLab(ConditionInfo conditionInfo) {
        return LABEL_PREFIX + conditionInfo.getFieldName() + conditionInfo.getOperator().name();
    }

    /**
     * 根据数据类型追加ExpressionAttributeValues
     *
     * @param valueLab                  key
     * @param conditionInfo             conditionInfo
     * @param value                     value
     * @param expressionAttributeValues expressionAttributeValues
     */
    private void appendExpressionAttributeValues(String valueLab, ConditionInfo conditionInfo, Object value, final Map<String, AttributeValue> expressionAttributeValues) {
        // 仅下推了N和S类型的filter
        switch (conditionInfo.getDynamoDbAttributeType()) {
            case N:
                expressionAttributeValues.put(valueLab, new AttributeValue().withN(value.toString()));
                break;
            case S:
                expressionAttributeValues.put(valueLab, new AttributeValue().withS(value.toString()));
                break;
            default:
                throw new IllegalArgumentException(String.format("Only support N or S type, not support %s. filedName is %s", conditionInfo.getDynamoDbAttributeType().name(), conditionInfo.getFieldName()));
        }
    }

    /**
     * 执行request获取结果集合
     *
     * @param queryRequest request
     * @return list
     */
    private List<Map<String, AttributeValue>> executeQuery(QueryRequest queryRequest) {
        List<Map<String, AttributeValue>> values = new ArrayList<>();
        queryRequest.withExclusiveStartKey(null);
        do {
            QueryResult result = dynamoDbClient.query(queryRequest);
            values.addAll(result.getItems());

            queryRequest.withExclusiveStartKey(result.getLastEvaluatedKey());
        } while (queryRequest.getExclusiveStartKey() != null);
        return values;
    }

    /**
     * 执行scan获取结果集合,其受限于scanlimit约束
     *
     * @param scanRequest request
     * @return list
     */
    private List<Map<String, AttributeValue>> executeScan(ScanRequest scanRequest) {
        List<Map<String, AttributeValue>> values = new ArrayList<>();
        scanRequest.withExclusiveStartKey(null);
        do {
            ScanResult result = dynamoDbClient.scan(scanRequest);
            values.addAll(result.getItems());

            scanRequest.withExclusiveStartKey(result.getLastEvaluatedKey());
            if (values.size() >= dynamoDbConfig.getScanLimit()) {
                break;
            }
        } while (scanRequest.getExclusiveStartKey() != null);
        return values.size() > dynamoDbConfig.getScanLimit() ? values.subList(0, dynamoDbConfig.getScanLimit()) : values;
    }

    private boolean checkSchema(String schemaName) {
        if (!DEFAULT_SCHEMA.equals(schemaName)) {
            throw new PrestoException(StandardErrorCode.GENERIC_USER_ERROR, String.format("Schema %s does not exist", schemaName));
        }
        return true;
    }

    public void shutdown() {
        dynamoDb.shutdown();
        dynamoDbClient.shutdown();
    }
}