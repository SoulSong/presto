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

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperTableModel;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMappingException;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.dynamodb.DynamoDbColumn;
import com.facebook.presto.dynamodb.DynamoDbConfig;
import com.facebook.presto.dynamodb.DynamoDbIndex;
import com.facebook.presto.dynamodb.DynamoDbTable;
import com.google.common.base.Stopwatch;
import org.apache.commons.collections.CollectionUtils;
import org.reflections.Reflections;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class LoadByJarTableRegister implements TableRegister{

    private static final Logger log = Logger.get(LoadByJarTableRegister.class);

    private final DynamoDbConfig dynamoDbConfig;
    private final DynamoDBMapper dynamoDbMapper;
    private final DynamoDB dynamoDb;
    private final List<String> schemaNames;

    public LoadByJarTableRegister(DynamoDbConfig dynamoDbConfig,DynamoDBMapper dynamoDbMapper,
                                  DynamoDB dynamoDb, List<String> schemaNames) {
        this.dynamoDbConfig = dynamoDbConfig;
        this.dynamoDbMapper = dynamoDbMapper;
        this.dynamoDb = dynamoDb;
        this.schemaNames = schemaNames;
    }

    /**
     * ???????????????????????????????????????
     *
     * @return {@link DynamoDbTable}
     */
    @Override
    public void register(Map<String, DynamoDbTable> tableMap) {
        log.info("Start to scan the table definition");
        Stopwatch stopwatch = Stopwatch.createStarted();
        schemaNames.forEach(schemaName -> {
            Arrays.stream(dynamoDbConfig.getPackageNames().split(",")).forEach(packageName -> {
                Reflections reflections = new Reflections(packageName);
                Set<Class<?>> classes = reflections.getTypesAnnotatedWith(DynamoDBTable.class);
                classes.forEach(entityClazz -> {
                    try {
                        String tableName = entityClazz.getAnnotation(DynamoDBTable.class).tableName();
                        DynamoDBMapperTableModel<?> tableModel = dynamoDbMapper.getTableModel(entityClazz);
                        if (tableModel == null) {
                            log.warn("Found no table by entity [%s]", entityClazz);
                            return;
                        }
                        String hashKeyName;
                        try {
                            DynamoDBMapperFieldModel hashKey = tableModel.hashKey();
                            hashKeyName = hashKey.name().toLowerCase(Locale.ENGLISH);
                        } catch (DynamoDBMappingException dynamoDBMappingException) {
                            log.warn("table [%s] has no hashKey definition in the entity class [%s].", tableName, entityClazz);
                            return;
                        }

                        // ??????Entity?????????????????????????????????
                        Map<String, DynamoDbColumn> allFields = tableModel.fields().stream().map(fieldModel -> {
                            String fieldName = fieldModel.name();
                            DynamoDBMapperFieldModel.DynamoDBAttributeType attributeType = fieldModel.attributeType();
                            Type prestoType = dynamoDbAttributeTypeToPrestoType(attributeType.name());
                            return new DynamoDbColumn(fieldName.toLowerCase(Locale.ENGLISH), fieldName, prestoType, attributeType);
                        }).collect(Collectors.toMap(DynamoDbColumn::getLowerCaseName, field -> field));

                        // ????????????
                        Map<String, DynamoDbIndex> dynamoDbIndexMap = new HashMap<>();

                        // ???????????????
                        Set<String> allProjections = allFields.values().stream().map(DynamoDbColumn::getOriginName).collect(Collectors.toSet());

                        String rangKeyName = null;
                        try {
                            DynamoDBMapperFieldModel rangeKey = tableModel.rangeKey();
                            if (rangeKey != null) {
                                rangKeyName = rangeKey.name().toLowerCase(Locale.ENGLISH);
                            }
                        } catch (DynamoDBMappingException ignore) {
                            // ?????? no mapping for range key ?????????????????????????????????rangeKey
                        }

                        // ???????????????????????????????????????indexName???key????????????PRIMARY
                        dynamoDbIndexMap.put(PRIMARY, new DynamoDbIndex(PRIMARY, hashKeyName, rangKeyName, allProjections));

                        // ??????????????????
                        try {
                            TableDescription tableDescription = dynamoDb.getTable(tableName).describe();

                            // ????????????????????????
                            List<LocalSecondaryIndexDescription> localSecondaryIndices = tableDescription.getLocalSecondaryIndexes();
                            if (CollectionUtils.isNotEmpty(localSecondaryIndices)) {
                                localSecondaryIndices.forEach(localSecondaryIndex -> {
                                    String indexName = localSecondaryIndex.getIndexName();
                                    dynamoDbIndexMap.put(indexName, buildDynamoDbIndexByIndexDesc(indexName, allProjections,
                                            localSecondaryIndex.getProjection(), localSecondaryIndex.getKeySchema()));
                                });
                            }

                            // ????????????????????????
                            List<GlobalSecondaryIndexDescription> globalSecondaryIndices = tableDescription.getGlobalSecondaryIndexes();
                            if (CollectionUtils.isNotEmpty(globalSecondaryIndices)) {
                                globalSecondaryIndices.forEach(globalSecondaryIndex -> {
                                    String indexName = globalSecondaryIndex.getIndexName();
                                    dynamoDbIndexMap.put(indexName, buildDynamoDbIndexByIndexDesc(indexName, allProjections,
                                            globalSecondaryIndex.getProjection(), globalSecondaryIndex.getKeySchema()));
                                });
                            }
                        } catch (ResourceNotFoundException exception) {
                            log.warn("table [%s] not found.", tableName);
                            return;
                        }

                        DynamoDbTable currentDynamoDbTable = new DynamoDbTable(schemaName, tableName.toLowerCase(Locale.ENGLISH), tableName,
                                allFields, hashKeyName, rangKeyName, dynamoDbIndexMap);
                        // ??????????????????????????????allFields??????????????????
                        DynamoDbTable temp = tableMap.get(currentDynamoDbTable.getLowerCaseTableName());
                        if (temp != null) {
                            if (temp.getColumns().size() < currentDynamoDbTable.getColumns().size()) {
                                tableMap.put(currentDynamoDbTable.getLowerCaseTableName(), currentDynamoDbTable);
                            }
                        } else {
                            tableMap.put(currentDynamoDbTable.getLowerCaseTableName(), currentDynamoDbTable);
                        }
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                });
            });
        });
        stopwatch.stop();
        log.info("load table schemas used {%s} ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }


    /**
     * ??????index?????????????????????{@link DynamoDbIndex}??????
     *
     * @param indexName         ?????????
     * @param allProjections    ???????????????
     * @param projection        ??????????????????????????????
     * @param keySchemaElements keySchemaElements
     * @return {@link DynamoDbIndex}
     */
    private DynamoDbIndex buildDynamoDbIndexByIndexDesc(String indexName, Set<String> allProjections,
                                                        Projection projection, List<KeySchemaElement> keySchemaElements) {
        // ?????????????????????projection???KeySchemaElement
        Set<String> projections = new HashSet<>();
        switch (projection.getProjectionType()) {
            case "INCLUDE":
                projections.addAll(projection.getNonKeyAttributes());
            case "KEYS_ONLY":
                keySchemaElements.forEach(keySchemaElement -> {
                    projections.add(keySchemaElement.getAttributeName());
                });
                break;
            case "ALL":
            default:
                projections.addAll(allProjections);
        }

        String hashKey = null;
        String rangeKey = null;
        for (KeySchemaElement keySchemaElement : keySchemaElements) {
            if (KeyType.HASH.name().equals(keySchemaElement.getKeyType())) {
                hashKey = keySchemaElement.getAttributeName();
            } else {
                rangeKey = keySchemaElement.getAttributeName();
            }
        }
        return new DynamoDbIndex(indexName, hashKey, rangeKey, projections);
    }
}
