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

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.dynamodb.DynamoDbColumn;
import com.facebook.presto.dynamodb.DynamoDbConfig;
import com.facebook.presto.dynamodb.DynamoDbIndex;
import com.facebook.presto.dynamodb.DynamoDbTable;
import com.facebook.presto.dynamodb.entity.FiledInfo;
import com.facebook.presto.dynamodb.entity.TableInfo;
import com.facebook.presto.dynamodb.exception.DynamodbMetadataErrorException;
import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.MapUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.gson.FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES;

public class LoadByHttpTableRegister implements TableRegister {

    private static final Logger log = Logger.get(LoadByHttpTableRegister.class);

    private final DynamoDbConfig dynamoDbConfig;
    private final static Gson GSON = new GsonBuilder().setFieldNamingStrategy(LOWER_CASE_WITH_UNDERSCORES).create();
    private final CloseableHttpClient httpClient;
    private final static String STATUS = "status";
    private final static String SUCCESS = "success";
    private final static String FAIL = "fail";
    private final static String METADATA_ID_KEY = "metadata_id";
    private final static String METADATA_ID_VALUE = "2fbb132a-6509-47bc-96e4-fba587529781";
    private final static String SKIP_CACHE = "skip_cache";
    private final static String _ALL_FIELDS_ = "_ALL_FIELDS_";

    private final List<String> schemaNames;

    public LoadByHttpTableRegister(DynamoDbConfig dynamoDbConfig, List<String> schemaNames) {
        this.dynamoDbConfig = dynamoDbConfig;
        this.schemaNames = schemaNames;
        httpClient = HttpClientBuilder.create().build();
    }

    @Override
    public void register(Map<String, DynamoDbTable> tableMap) {
        tableMap.putAll(parseTableInfo());
        // 定时刷新元数据信息
        if (dynamoDbConfig.getRefreshSeconds() > 0) {
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
                log.info("start to refresh table metadata.");
                Map<String, DynamoDbTable> newTableMaps = parseTableInfo();
                if (MapUtils.isEmpty(newTableMaps)) {
                    log.warn("end of refreshing table metadata unsuccessfully.");
                } else {
                    // 获取被删除的表清单
                    Set<String> removeTables = tableMap.keySet();
                    removeTables.removeAll(newTableMaps.keySet());
                    removeTables.forEach(tableMap::remove);
                    // 更新剩下的表信息
                    tableMap.putAll(newTableMaps);
                    log.info("end of refreshing table metadata successfully.");
                }
            }, dynamoDbConfig.getRefreshSeconds(), dynamoDbConfig.getRefreshSeconds(), TimeUnit.SECONDS);
        }
    }

    /**
     * 解析元数据为dynamodb特征的结构
     *
     * @return key是小写表名，value为{@link DynamoDbTable}
     */
    public Map<String, DynamoDbTable> parseTableInfo() {
        log.info("Start to scan the table definition");
        Stopwatch stopwatch = Stopwatch.createStarted();
        Map<String, DynamoDbTable> tableMap = new HashMap<>();
        schemaNames.forEach(schemaName -> {
            List<TableInfo> tableInfoList = requestTableInfo();
            tableInfoList.forEach(tableInfo -> {
                tableInfo.setLowercaseTableName(tableInfo.getTableName().toLowerCase(Locale.ENGLISH));
                tableMap.put(tableInfo.getLowercaseTableName(), buildDynamoDbTable(schemaName, tableInfo));
            });
        });
        stopwatch.stop();
        log.info("load %d table schemas used %s ms", tableMap.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return tableMap;
    }

    /**
     * 从远端获取{@link TableInfo}信息
     *
     * @return list of tableInfo
     */
    private List<TableInfo> requestTableInfo() {
        List<TableInfo> tableInfoList = new ArrayList<>();
        CloseableHttpResponse response = null;
        try {
            HttpPost httpPost = new HttpPost(dynamoDbConfig.getSchemaUri());
            httpPost.addHeader("Content-Type", "application/json");
            Map<String, Object> requestBody = new HashMap<>(2);
            requestBody.put(METADATA_ID_KEY, METADATA_ID_VALUE);
            requestBody.put(SKIP_CACHE, false);
            httpPost.setEntity(new StringEntity(GSON.toJson(requestBody)));

            response = httpClient.execute(httpPost);
            String responseContent = EntityUtils.toString(response.getEntity(), "UTF-8");
            if (response.getStatusLine().getStatusCode() == 200) {
                Map<String, Object> result = GSON.fromJson(responseContent, Map.class);
                Object body = result.get("body");
                if (body != null && result.getOrDefault(STATUS, FAIL).equals(SUCCESS)) {
                    tableInfoList = GSON.fromJson(GSON.toJson(body), new TypeToken<List<TableInfo>>() {
                    }.getType());
                }
            } else {
                log.warn("request %s fail, http status %s , response : %s", dynamoDbConfig.getSchemaUri(),
                        response.getStatusLine().getStatusCode(), responseContent);
            }
        } catch (IOException e) {
            log.error(e, "request %s fail.message : %s", dynamoDbConfig.getSchemaUri(), e.getMessage());
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                log.error(e, "release resource error, message : %s", e.getMessage());
            }
        }
        return tableInfoList;
    }


    /**
     * 基于通用{@link TableInfo}构造{@link DynamoDbTable}
     *
     * @param tableInfo tableInfo
     * @return {@link DynamoDbTable}
     */
    private DynamoDbTable buildDynamoDbTable(String schemaName, TableInfo tableInfo) {
        Map<String, DynamoDbIndex> dynamoDbIndices = buildDynamoDbIndices(tableInfo);
        DynamoDbIndex primaryIndex = dynamoDbIndices.get(PRIMARY);
        if (primaryIndex == null) {
            throw new DynamodbMetadataErrorException(String.format("Dynamodb table [%s] has no primary index.", tableInfo.getTableName()));
        }
        List<DynamoDbColumn> dynamoDbColumns = buildDynamoDbColumns(tableInfo);

        return new DynamoDbTable(schemaName,
                tableInfo.getTableName().toLowerCase(Locale.ENGLISH),
                tableInfo.getTableName(),
                dynamoDbColumns.stream()
                        .collect(Collectors.toMap(DynamoDbColumn::getLowerCaseName, dynamoDbColumn -> dynamoDbColumn, (a,b)-> a)),
                primaryIndex.getHashKey(), primaryIndex.getRangeKey(), dynamoDbIndices);
    }

    /**
     * 基于通用{@link TableInfo}构造{@link DynamoDbIndex}
     *
     * @param tableInfo tableInfo
     * @return {@link DynamoDbIndex}
     */
    private Map<String, DynamoDbIndex> buildDynamoDbIndices(TableInfo tableInfo) {
        return tableInfo.getIndexInfos().stream()
                .map(indexInfo -> {
                    String indexName = indexInfo.getIndexName();
                    String hashKey = null;
                    String rangeKey = null;
                    List<String> keys = indexInfo.getKeys();
                    if (keys.size() == 1) {
                        hashKey = keys.get(0);
                    } else if (keys.size() == 2) {
                        hashKey = keys.get(0);
                        rangeKey = keys.get(1);
                    } else {
                        throw new DynamodbMetadataErrorException(String.format("Dynamodb table [%s] index [%s] keys [%s] error.", tableInfo.getTableName()
                                , indexInfo.getIndexName(), keys));
                    }
                    Set<String> projections;
                    // 如果是主索引则进行默认投影全字段
                    if (PRIMARY.equals(indexInfo.getIndexName())) {
                        projections = tableInfo.getFields().stream()
                                .map(FiledInfo::getFieldName)
                                .collect(Collectors.toSet());
                    } else {
                        // 追加hashKey和rangeKey
                        projections = new HashSet<>(indexInfo.getKeys());
                        if (indexInfo.getProjections() != null) {
                            Set<String> temp = new HashSet<>(indexInfo.getProjections());
                            // 如果存在约定的全字段标示则加载所有field
                            if (temp.contains(_ALL_FIELDS_)) {
                                projections.addAll(tableInfo.getFields().stream()
                                        .map(FiledInfo::getFieldName)
                                        .collect(Collectors.toSet()));
                            }
                            projections.addAll(temp);
                        }
                    }
                    return new DynamoDbIndex(indexName, hashKey, rangeKey, projections);
                }).collect(Collectors.toMap(DynamoDbIndex::getIndexName, dynamoDbIndex -> dynamoDbIndex));
    }

    /**
     * 基于通用{@link TableInfo}构造{@link DynamoDbColumn}
     *
     * @param tableInfo tableInfo
     * @return {@link DynamoDbColumn}
     */
    private List<DynamoDbColumn> buildDynamoDbColumns(TableInfo tableInfo) {
        return tableInfo.getFields().stream().map(filedInfo -> {
            try {
                String fieldName = filedInfo.getFieldName();
                DynamoDBMapperFieldModel.DynamoDBAttributeType dynamoDbAttributeType = DynamoDBMapperFieldModel.DynamoDBAttributeType
                        .valueOf(filedInfo.getDbType().toUpperCase(Locale.ENGLISH));
                Type prestoType = dynamoDbAttributeTypeToPrestoType(dynamoDbAttributeType.name());
                return new DynamoDbColumn(fieldName.toLowerCase(Locale.ENGLISH), fieldName, prestoType, dynamoDbAttributeType);
            } catch (Exception e) {
                throw new DynamodbMetadataErrorException(String.format("Dynamodb table [%s] field [%s] type [%s] error.", tableInfo.getTableName()
                        , filedInfo.getFieldName(), filedInfo.getDbType()));
            }
        }).collect(Collectors.toList());
    }

}
