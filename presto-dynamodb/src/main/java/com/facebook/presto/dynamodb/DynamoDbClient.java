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
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections.CollectionUtils;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
 * ??????tableName???fieldName??????????????????????????????????????????originName?????????????????????queryExpression?????????????????????
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
     * key???tableName
     * value??? {@link DynamoDbTable}
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
     * ????????????Default??????
     *
     * @return schemaNames
     */
    public List<String> getSchemaNames() {
        return ImmutableList.of(DEFAULT_SCHEMA);
    }

    /**
     * ????????????schema????????????tableNames
     *
     * @param schema ??????schema
     * @return tableNames
     */
    public Set<String> getTableNames(String schema) {
        checkSchema(schema);
        return ImmutableSet.copyOf(tableMap.keySet());
    }

    /**
     * ???????????????
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
     * ???????????????sql?????????????????????????????????project???filter??????
     *
     * @param split {@link DynamoDbSplit}
     * @return ??????????????????
     */
    public Iterator<Map<String, AttributeValue>> getTableData(DynamoDbSplit split) {
        Optional<Map<ColumnHandle, Domain>> domains = split.getTupleDomain().getDomains();
        if (domains.isPresent()) {
            List<ConditionInfo> conditionInfos = PredicateParser.translateConstraintToCondition(domains.get());
            String schemaName = split.getSchemaName();
            String tableName = split.getTableName();
            Set<DynamoDbColumn> desiredColumns = split.getDesiredColumns();
            DynamoDbTable table = tableMap.get(tableName);

            // ????????????????????????????????????????????????????????????????????????config????????????
            if (CollectionUtils.isEmpty(conditionInfos)) {
                ScanRequest scanRequest = buildScanRequest(table, desiredColumns);
                return executeScan(scanRequest).iterator();
            } else if (conditionInfos.size() == 1 && ComparisonOperator.IN.equals(conditionInfos.get(0).getOperator())) {
                // ??????????????????in?????????
                ConditionInfo conditionInfo = conditionInfos.get(0);
                // ??????table???primaryIndex????????????hashKey??????rangekey???,???in????????????????????????????????????????????????batchGet
                // batchGet??????????????????????????????????????????????????????
                if (table.getHashKeyName().equalsIgnoreCase(conditionInfo.getFieldName()) && table.getRangeKeyName() == null) {
                    BatchGetItemResult result = dynamoDbClient.batchGetItem(buildBatchGetItemRequestWithHashKey(conditionInfo, tableName, desiredColumns));
                    log.info("fetch result size is %s", result.getResponses().get(tableName).size());
                    return result.getResponses().get(tableName).iterator();
                } else {
                    // ????????????in??????????????????????????????????????????????????????????????????
                    List<Map<String, AttributeValue>> result = new ArrayList<>();
                    conditionInfo.getFieldValues().forEach(fieldValue -> {
                        ConditionInfo targetConditionInfo = new ConditionInfo(conditionInfo.getFieldName(), true,
                                fieldValue, null, conditionInfo.getDynamoDbAttributeType(), ComparisonOperator.EQ);
                        QueryRequest queryRequest = buildQueryRequest(table, new ArrayList<>(Collections.singletonList(targetConditionInfo)), desiredColumns);
                        result.addAll(executeQuery(queryRequest));
                    });
                    return result.iterator();
                }
            } else if (conditionInfos.size() == 2) {
                Set<ComparisonOperator> comparisonOperators = new HashSet<>(2);
                conditionInfos.forEach(conditionInfo -> comparisonOperators.add(conditionInfo.getOperator()));
                // ?????????????????????????????????????????????????????????in??????????????????????????????primaryIndex??????hashKey???rangeKey????????????????????????batchGet????????????
                if (comparisonOperators.containsAll(ImmutableSet.of(ComparisonOperator.IN, ComparisonOperator.EQ)) && table.getRangeKeyName() != null) {
                    ConditionInfo eqConditionInfo = conditionInfos.stream().filter(conditionInfo -> ComparisonOperator.EQ.equals(conditionInfo.getOperator())).findFirst().orElse(null);
                    ConditionInfo inConditionInfo = conditionInfos.stream().filter(conditionInfo -> ComparisonOperator.IN.equals(conditionInfo.getOperator())).findFirst().orElse(null);
                    Set<String> keyFieldNames = new HashSet<>(2);
                    keyFieldNames.add(table.getHashKeyName().toLowerCase(Locale.ENGLISH));
                    keyFieldNames.add(table.getRangeKeyName().toLowerCase(Locale.ENGLISH));
                    if (eqConditionInfo != null && inConditionInfo != null
                            && keyFieldNames.containsAll(ImmutableSet.of(eqConditionInfo.getFieldName().toLowerCase(Locale.ENGLISH),
                            inConditionInfo.getFieldName().toLowerCase(Locale.ENGLISH)))) {
                        BatchGetItemResult result = dynamoDbClient.batchGetItem(buildBatchGetItemRequestWithHashKeyAndRangeKey(eqConditionInfo, inConditionInfo, tableName, desiredColumns));
                        log.info("fetch result size is %s", result.getResponses().get(tableName).size());
                        return result.getResponses().get(tableName).iterator();
                    }
                }
            }

            // ??????rbo??????????????????query
            QueryRequest queryRequest = buildQueryRequest(table, conditionInfos, desiredColumns);
            return executeQuery(queryRequest).iterator();
        }

        throw new NotSupportException("Not support scan table.");
    }


    private ScanRequest buildScanRequest(DynamoDbTable table, Set<DynamoDbColumn> desiredColumns) {
        // ??????projection
        Set<String> projections = desiredColumns.stream().map(DynamoDbColumn::getOriginName).collect(Collectors.toSet());
        // ??????QueryRequest
        String originTableName = table.getOriginTableName();
        ScanRequest scanRequest = new ScanRequest();
        scanRequest.withTableName(originTableName)
                .withLimit(dynamoDbConfig.getLimit());

        // ????????????count(*)?????????projections???empty
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
     * ????????????hashKey??????{@link BatchGetItemRequest}??????????????????index
     *
     * @param conditionInfo  conditionInfo
     * @param tableName      tableName
     * @param desiredColumns ????????????????????????????????????
     * @return BatchGetItemRequest
     */
    private BatchGetItemRequest buildBatchGetItemRequestWithHashKey(ConditionInfo conditionInfo, String tableName,
                                                                    Set<DynamoDbColumn> desiredColumns) {
        BatchGetItemRequest batchGetItemRequest = new BatchGetItemRequest();
        // ?????????????????????
        Set<String> projections = desiredColumns.stream().map(DynamoDbColumn::getOriginName).collect(Collectors.toSet());
        Map<String, String> expressionAttributeNames = new HashMap<>(projections.size());
        projections.forEach(projection -> expressionAttributeNames.put(POUND_SIGN.concat(projection.toLowerCase(Locale.ENGLISH)), projection));

        String fieldName = conditionInfo.getFieldName();

        KeysAndAttributes keysAndAttributes = new KeysAndAttributes();
        List<Map<String, AttributeValue>> keys = new ArrayList<>();
        switch (conditionInfo.getDynamoDbAttributeType()) {
            case N:
                conditionInfo.getFieldValues().forEach(value -> {
                    keys.add(ImmutableMap.of(fieldName, new AttributeValue().withN(value.toString())));
                });
                break;
            case S:
                conditionInfo.getFieldValues().forEach(value -> {
                    keys.add(ImmutableMap.of(fieldName, new AttributeValue().withS(value.toString())));
                });
                break;
            default:
                throw new IllegalArgumentException(String.format("HashKey and rangeKey only support N or S type, not support %s. filedName is %s",
                        conditionInfo.getDynamoDbAttributeType().name(), conditionInfo.getFieldName()));
        }
        keysAndAttributes.withKeys(keys);

        keysAndAttributes.withProjectionExpression(projections.stream().map(projection -> POUND_SIGN.concat(projection.toLowerCase(Locale.ENGLISH))).collect(Collectors.joining(IN_DELIMITER)))
                .withExpressionAttributeNames(expressionAttributeNames);
        batchGetItemRequest.withRequestItems(ImmutableMap.of(tableName, keysAndAttributes));
        return batchGetItemRequest;
    }

    /**
     * ????????????hashKey???rangeKey??????{@link BatchGetItemRequest}??????????????????index
     *
     * @param eqConditionInfo eqConditionInfo
     * @param inConditionInfo inConditionInfo
     * @param tableName       tableName
     * @param desiredColumns  ????????????????????????????????????
     * @return BatchGetItemRequest
     */
    private BatchGetItemRequest buildBatchGetItemRequestWithHashKeyAndRangeKey(ConditionInfo eqConditionInfo, ConditionInfo inConditionInfo,
                                                                               String tableName, Set<DynamoDbColumn> desiredColumns) {
        BatchGetItemRequest batchGetItemRequest = new BatchGetItemRequest();
        // ?????????????????????
        Set<String> projections = desiredColumns.stream().map(DynamoDbColumn::getOriginName).collect(Collectors.toSet());
        Map<String, String> expressionAttributeNames = new HashMap<>(projections.size());
        projections.forEach(projection -> expressionAttributeNames.put(POUND_SIGN.concat(projection.toLowerCase(Locale.ENGLISH)), projection));

        String inFieldName = inConditionInfo.getFieldName();

        KeysAndAttributes keysAndAttributes = new KeysAndAttributes();
        List<Map<String, AttributeValue>> keys = new ArrayList<>();
        final AttributeValue eqConditionValue = new AttributeValue();
        switch (eqConditionInfo.getDynamoDbAttributeType()) {
            case N:
                eqConditionValue.withN(eqConditionInfo.getFieldValue().toString());
                break;
            case S:
                eqConditionValue.withS(eqConditionInfo.getFieldValue().toString());
                break;
            default:
                throw new IllegalArgumentException(String.format("HashKey and rangeKey only support N or S type, not support %s. filedName is %s",
                        inConditionInfo.getDynamoDbAttributeType().name(), inConditionInfo.getFieldName()));
        }

        switch (inConditionInfo.getDynamoDbAttributeType()) {
            case N:
                inConditionInfo.getFieldValues().forEach(value -> {
                    keys.add(ImmutableMap.of(eqConditionInfo.getFieldName(), eqConditionValue, inFieldName, new AttributeValue().withN(value.toString())));
                });
                break;
            case S:
                inConditionInfo.getFieldValues().forEach(value -> {
                    keys.add(ImmutableMap.of(eqConditionInfo.getFieldName(), eqConditionValue, inFieldName, new AttributeValue().withS(value.toString())));
                });
                break;
            default:
                throw new IllegalArgumentException(String.format("HashKey and rangeKey only support N or S type, not support %s. filedName is %s",
                        inConditionInfo.getDynamoDbAttributeType().name(), inConditionInfo.getFieldName()));
        }
        keysAndAttributes.withKeys(keys);

        keysAndAttributes.withProjectionExpression(projections.stream().map(projection -> POUND_SIGN.concat(projection.toLowerCase(Locale.ENGLISH))).collect(Collectors.joining(IN_DELIMITER)))
                .withExpressionAttributeNames(expressionAttributeNames);
        batchGetItemRequest.withRequestItems(ImmutableMap.of(tableName, keysAndAttributes));
        return batchGetItemRequest;
    }


    /**
     * ????????????????????????filter???projection????????????dynamodb query request
     *
     * @param table             tableName,???????????????????????????????????????
     * @param allConditionInfos ??????????????????condition
     * @param desiredColumns    ???????????????
     * @return {@link QueryRequest}
     */
    private QueryRequest buildQueryRequest(DynamoDbTable table, List<ConditionInfo> allConditionInfos, Set<DynamoDbColumn> desiredColumns) {
        // hashKey?????????????????????
        Set<String> hashKeyCandidateFieldSet = allConditionInfos.stream().filter(ConditionInfo::getEqualOperator).map(ConditionInfo::getFieldName).collect(Collectors.toSet());

        // ?????????????????????????????????????????????????????????????????????hashKey???query??????
        if (CollectionUtils.isEmpty(hashKeyCandidateFieldSet)) {
            throw new NotSupportException("Not support scan table.");
        }

        // rangeKey????????????=???<???<=???>???>=?????????,???????????????????????????????????????;
        Set<String> rangeKeyCandidateFieldSet = allConditionInfos.stream().filter(conditionInfo -> ComparisonOperator.EQ.equals(conditionInfo.getOperator()) || ComparisonOperator.GT.equals(conditionInfo.getOperator()) || ComparisonOperator.GE.equals(conditionInfo.getOperator()) || ComparisonOperator.LT.equals(conditionInfo.getOperator()) || ComparisonOperator.LE.equals(conditionInfo.getOperator())).collect(Collectors.groupingBy(ConditionInfo::getFieldName, Collectors.counting())).entrySet().stream().filter(entry -> entry.getValue() == 1).map(Map.Entry::getKey).collect(Collectors.toSet());
        // ??????hashKey???rangeKey?????????
        Map<String, ConditionInfo> allSingleConditionInfoMap = allConditionInfos.stream().filter(conditionInfo -> hashKeyCandidateFieldSet.contains(conditionInfo.getFieldName()) || rangeKeyCandidateFieldSet.contains(conditionInfo.getFieldName())).collect(Collectors.toMap(ConditionInfo::getFieldName, info -> info));

        // ??????projection
        Set<String> projections = desiredColumns.stream().map(DynamoDbColumn::getOriginName).collect(Collectors.toSet());
        Map<String, String> expressionAttributeNames = new HashMap<>(projections.size());
        projections.forEach(projection -> expressionAttributeNames.put(POUND_SIGN.concat(projection.toLowerCase(Locale.ENGLISH)), projection));

        // ??????QueryRequest
        String originTableName = table.getOriginTableName();
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.withTableName(originTableName)
                .withLimit(dynamoDbConfig.getLimit())
                .withProjectionExpression(projections.stream()
                        .map(projection -> POUND_SIGN.concat(projection.toLowerCase(Locale.ENGLISH)))
                        .collect(Collectors.joining(IN_DELIMITER)))
                .withExpressionAttributeNames(expressionAttributeNames);

        // ??????hashKey&rangeKey????????????????????????????????????
        String indexName = findBestMatchIndex(table, hashKeyCandidateFieldSet, rangeKeyCandidateFieldSet, allSingleConditionInfoMap, projections);
        if (NONE_INDEX.equals(indexName)) {
            throw new NotSupportException("No suitable hashKey filter item in the current sql.");
        }
        log.info("query table : %s , index : %s", originTableName, indexName);
        queryRequest.withIndexName(PRIMARY.equals(indexName) ? null : indexName);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        queryRequest.withExpressionAttributeValues(expressionAttributeValues);

        //???????????????hashKey???rangeKey
        DynamoDbIndex bestMatchIndex = table.getDynamoDbIndexMap().get(indexName);
        String hashKey = bestMatchIndex.getHashKey();
        String rangeKey = bestMatchIndex.getRangeKey();
        // ??????keyConditionExpression
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
        // ??????keyCondition?????????????????????filterExpression??????
        if (allConditionInfos.size() > 0) {
            List<String> filterConditionExpressions = new ArrayList<>(allConditionInfos.size());
            allConditionInfos.forEach(conditionInfo -> {
                // rangeKey??????????????????filterExpression???,????????????
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
     * ??????hashKey&rangeKey???????????????????????????????????????index????????????????????????????????????
     * ???????????????globalFactor??????????????????????????????1???score= globalFactor*(score_hashKey+score_projections+score_rangeKey)
     * 1???hashKey:????????????????????????????????????10?????????????????????????????????????????????globalFactor???0???
     * 2???projections:???????????????4??????????????????3?????????????????????globalFactor???0???
     * 3???rangeKey:??????????????????????????????????????????????????????7???????????????????????????????????????????????????????????????5??????????????????????????????rangeKey??????0??????
     *
     * @param table                     ????????????
     * @param hashKeyCandidateFieldSet  hashKey???????????????filedName??????
     * @param rangeKeyCandidateFieldSet rangeKey???????????????filedName??????
     * @param allSingleConditionInfoMap hashKey&rangeKey?????????
     * @param projections               ??????????????????
     * @return ?????????indexName??????????????????????????????0???????????????????????? NONE_INDEX
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
        // ????????????????????????
        List<Map.Entry<String, Integer>> list = candidates.entrySet().stream().filter(entry -> entry.getValue() > 0)
                .sorted(Comparator.comparing(entry -> ((Map.Entry<String, Integer>) entry).getValue()).reversed()).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(list)) {
            // ???????????????
            return list.get(0).getKey();
        }

        return NONE_INDEX;
    }

    /**
     * ??????condition??????expression???????????????expressionValues
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
                // TODO ???????????????????????????
                throw new IllegalArgumentException(String.format("Not support %s operator in the filter expression.", conditionInfo.getOperator().name()));
        }
    }

    /**
     * ??????conditionInfo??????expressionValue???key??????
     *
     * @param conditionInfo conditionInfo
     * @return key
     */
    private String buildValueLab(ConditionInfo conditionInfo) {
        return LABEL_PREFIX + conditionInfo.getFieldName() + conditionInfo.getOperator().name();
    }

    /**
     * ????????????????????????ExpressionAttributeValues
     *
     * @param valueLab                  key
     * @param conditionInfo             conditionInfo
     * @param value                     value
     * @param expressionAttributeValues expressionAttributeValues
     */
    private void appendExpressionAttributeValues(String valueLab, ConditionInfo conditionInfo, Object value, final Map<String, AttributeValue> expressionAttributeValues) {
        // ????????????N???S?????????filter
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
     * ??????request??????????????????
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
        log.info("ExecuteQuery fetch result size is %s", values.size());
        return values;
    }

    /**
     * ??????scan??????????????????,????????????scanlimit??????
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