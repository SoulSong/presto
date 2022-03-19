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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.dynamodb.entity.Music;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamoDbSampleTest extends AbstractDynamoDbTest {
    private static final Logger log = Logger.get(DynamoDbSampleTest.class);

    private static final String tableName = "t_Music";

    /**
     * 创建带有二级索引的样例表
     */
    @Test
    public void createTable() {
        dropTable(tableName);
        createTable(tableName);
    }

    /**
     * 初始化待查询数据集
     */
    @Test
    public void loadData() {
        dynamoDbMapper.batchSave(new Music("lin junjie", "caocao", "曹操", 2006)
                , new Music("lin junjie", "jiangnan", "江南", 2004)
                , new Music("lin junjie", "xijie", null, 2007)
                , new Music("lin junjie", "xindiqiu", "新地球", 2014)
                , new Music("zhou jielun", "jiangnan", "范特西", 2001)
                , new Music("zhou jielun", "badukongjian", "八度空间", 2002)
                , new Music("zhou jielun", "jingtanhao", "惊叹号", 2010)
                , new Music("zhou jielun", "wohenmang", "我很忙", 2007)
        );
        log.info("load success.");
    }


    /**
     * 模拟常见的集中查询方式(基于condition)，便于后续基于presto的实现参考
     */
    @Test
    public void query() {
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        Map<String, Condition> keyConditions = new HashMap<>(2);
        Map<String, Condition> filterConditions = new HashMap<>();
        List<String> projections = new ArrayList<>();
        projections.add("artist");
        projections.add("album");
        projections.add("fYear");
        projections.add("title");

        QueryRequest queryRequest = new QueryRequest()
                .withTableName(tableName)
                .withLimit(1)
                .withKeyConditions(keyConditions)
                .withQueryFilter(filterConditions)
                .withAttributesToGet(projections);

        List<Map<String, AttributeValue>> values = new ArrayList<>();

        // 提供完整的hashKey和rangKey，则直接通过load完成查询
        keyConditions.put("artist", new Condition().withAttributeValueList(new AttributeValue().withS("lin junjie")).withComparisonOperator(ComparisonOperator.EQ));
        keyConditions.put("title", new Condition().withAttributeValueList(new AttributeValue().withS("jiangnan")).withComparisonOperator(ComparisonOperator.EQ));
        execute(queryRequest, values);
        assert values.size() == 1;

        // 仅提供hashKey，指定projection投影字段
        keyConditions.clear();
        values.clear();
        projections.remove(3);
        queryRequest.withAttributesToGet(projections);
        keyConditions.put("artist", new Condition().withAttributeValueList(new AttributeValue().withS("lin junjie")).withComparisonOperator(ComparisonOperator.EQ));
        execute(queryRequest, values);
        assert values.size() == 4;

        // 仅提供hashKey，同时增加了非rangKey的过滤条件，并指定projection投影字段
        values.clear();
        filterConditions.put("fYear", new Condition().withAttributeValueList(new AttributeValue().withN("2006")).withComparisonOperator(ComparisonOperator.EQ));
        execute(queryRequest, values);
        assert values.size() == 1;

        // 仅提供hashKey，同时增加了非rangKey的复杂过滤条件，并指定projection投影字段
        values.clear();
        filterConditions.put("fYear", new Condition().withAttributeValueList(new AttributeValue().withN("2007")).withComparisonOperator(ComparisonOperator.LE));
        execute(queryRequest, values);
        assert values.size() == 2;

        // 查询二级索引，提供hashKey和rangKey，同时指定projection投影字段
        projections = new ArrayList<>();
        projections.add("artist");
        projections.add("fYear");
        projections.add("title");

        keyConditions.clear();
        values.clear();
        filterConditions.clear();
        queryRequest.withIndexName("fyear_artist_index");
        queryRequest.withAttributesToGet(projections);
        keyConditions.put("artist", new Condition().withAttributeValueList(new AttributeValue().withS("lin junjie")).withComparisonOperator(ComparisonOperator.EQ));
        keyConditions.put("fYear", new Condition().withAttributeValueList(new AttributeValue().withN("2007")).withComparisonOperator(ComparisonOperator.EQ));
        execute(queryRequest, values);
        assert values.size() == 1;

        // 查询二级索引，仅提供hashKey，同时指定projection投影字段
        keyConditions.clear();
        values.clear();
        filterConditions.clear();
        queryRequest.withIndexName("fyear_artist_index");
        keyConditions.put("fYear", new Condition().withAttributeValueList(new AttributeValue().withN("2007")).withComparisonOperator(ComparisonOperator.EQ));
        execute(queryRequest, values);
        assert values.size() == 2;
    }

    /**
     * 模拟常见的集中查询方式(基于expression)，便于后续基于presto的实现参考
     */
    @Test
    public void query2() {
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();

        QueryRequest queryRequest = new QueryRequest()
                .withTableName(tableName)
                .withLimit(1)
                .withProjectionExpression("artist,album,fYear,title");

        List<Map<String, AttributeValue>> values = new ArrayList<>();

        // 提供完整的hashKey和rangKey，则直接通过load完成查询
        expressionAttributeValues.put(":artist" + ComparisonOperator.EQ.name(), new AttributeValue().withS("lin junjie"));
        expressionAttributeValues.put(":title" + ComparisonOperator.EQ, new AttributeValue().withS("jiangnan"));
        queryRequest.withKeyConditionExpression("artist = :artist" + ComparisonOperator.EQ.name() + " and title = :title" + ComparisonOperator.EQ)
                .withExpressionAttributeValues(expressionAttributeValues);
        execute(queryRequest, values);
        assert values.size() == 1;

        // 仅提供hashKey，指定projection投影字段
        values.clear();
        expressionAttributeValues.clear();
        expressionAttributeValues.put(":artist" + ComparisonOperator.EQ.name(), new AttributeValue().withS("lin junjie"));
        queryRequest.withKeyConditionExpression("artist = :artist" + ComparisonOperator.EQ)
                .withProjectionExpression("artist,album,fYear");
        execute(queryRequest, values);
        assert values.size() == 4;

        // 仅提供hashKey，同时增加了非rangKey的过滤条件，并指定projection投影字段
        values.clear();
        expressionAttributeValues.clear();
        expressionAttributeValues.put(":artist" + ComparisonOperator.EQ.name(), new AttributeValue().withS("lin junjie"));
        expressionAttributeValues.put(":fYear" + ComparisonOperator.EQ.name(), new AttributeValue().withN("2006"));
        queryRequest.withKeyConditionExpression("artist = :artist" + ComparisonOperator.EQ)
                .withFilterExpression("fYear = :fYear" + ComparisonOperator.EQ.name())
                .withProjectionExpression("artist,album,fYear");
        execute(queryRequest, values);
        assert values.size() == 1;

        // 仅提供hashKey，同时增加了非rangKey的空过滤，并指定projection投影字段
        values.clear();
        expressionAttributeValues.clear();
        expressionAttributeValues.put(":artist" + ComparisonOperator.EQ.name(), new AttributeValue().withS("lin junjie"));
        queryRequest.withKeyConditionExpression("artist = :artist" + ComparisonOperator.EQ)
                .withFilterExpression(" attribute_not_exists(album)")
                .withProjectionExpression("artist,album,fYear");
        execute(queryRequest, values);
        assert values.size() == 1;

        // 仅提供hashKey，同时增加了非rangKey的非空过滤，并指定projection投影字段
        values.clear();
        expressionAttributeValues.clear();
        expressionAttributeValues.put(":artist" + ComparisonOperator.EQ.name(), new AttributeValue().withS("lin junjie"));
        queryRequest.withKeyConditionExpression("artist = :artist" + ComparisonOperator.EQ)
                .withFilterExpression(" attribute_exists(album)")
                .withProjectionExpression("artist,album,fYear");
        execute(queryRequest, values);
        assert values.size() == 3;

        // 仅提供hashKey，同时增加了非rangKey的复杂过滤条件，并指定projection投影字段
        values.clear();
        expressionAttributeValues.clear();
        expressionAttributeValues.put(":artist" + ComparisonOperator.EQ.name(), new AttributeValue().withS("lin junjie"));
        expressionAttributeValues.put(":fYear" + ComparisonOperator.NE.name(), new AttributeValue().withN("2007"));
        queryRequest.withKeyConditionExpression("artist = :artist" + ComparisonOperator.EQ.name())
                .withFilterExpression("fYear <> :fYear" + ComparisonOperator.NE.name())
                .withProjectionExpression("artist,album,fYear");
        execute(queryRequest, values);
        assert values.size() == 3;

        // 仅提供hashKey，同时增加了非rangKey的复杂过滤条件，并指定projection投影字段
        values.clear();
        expressionAttributeValues.clear();
        expressionAttributeValues.put(":artist" + ComparisonOperator.EQ.name(), new AttributeValue().withS("lin junjie"));
        expressionAttributeValues.put(":fYear" + ComparisonOperator.GT.name(), new AttributeValue().withN("2004"));
        expressionAttributeValues.put(":fYear" + ComparisonOperator.LE.name(), new AttributeValue().withN("2007"));
        queryRequest.withKeyConditionExpression("artist = :artist" + ComparisonOperator.EQ.name())
                .withFilterExpression("fYear > :fYear" + ComparisonOperator.GT.name() + " AND fYear <= :fYear" + ComparisonOperator.LE.name())
                .withProjectionExpression("artist,album,fYear");
        execute(queryRequest, values);
        assert values.size() == 2;

        // 仅提供hashKey，同时增加了非rangKey的复杂过滤条件，并指定projection投影字段
        values.clear();
        expressionAttributeValues.clear();
        expressionAttributeValues.put(":artist" + ComparisonOperator.EQ.name(), new AttributeValue().withS("lin junjie"));
        expressionAttributeValues.put(":fYear" + 1, new AttributeValue().withN("2004"));
        expressionAttributeValues.put(":fYear" + 2, new AttributeValue().withN("2007"));
        queryRequest.withKeyConditionExpression("artist = :artist" + ComparisonOperator.EQ.name())
                .withFilterExpression("fYear in (:fYear" + 1 + " , :fYear" + 2+")")
                .withProjectionExpression("artist,album,fYear");
        execute(queryRequest, values);
        assert values.size() == 2;

        // 查询二级索引，提供hashKey和rangKey，同时指定projection投影字段
        values.clear();
        expressionAttributeValues.clear();
        queryRequest.withIndexName("fyear_artist_index")
                .withProjectionExpression("artist,title,fYear")
                .withFilterExpression(null)
                .withKeyConditionExpression("artist = :artist" + ComparisonOperator.EQ.name() + " and fYear = :fYear" + ComparisonOperator.EQ.name());
        expressionAttributeValues.put(":artist" + ComparisonOperator.EQ.name(), new AttributeValue().withS("lin junjie"));
        expressionAttributeValues.put(":fYear" + ComparisonOperator.EQ.name(), new AttributeValue().withN("2007"));
        execute(queryRequest, values);
        assert values.size() == 1;

        // 查询二级索引，仅提供hashKey，同时指定projection投影字段
        values.clear();
        expressionAttributeValues.clear();
        queryRequest.withIndexName("fyear_artist_index")
                .withProjectionExpression("artist,title,fYear")
                .withFilterExpression(null)
                .withKeyConditionExpression("fYear = :fYear" + ComparisonOperator.EQ.name());
        expressionAttributeValues.put(":fYear" + ComparisonOperator.EQ.name(), new AttributeValue().withN("2007"));
        execute(queryRequest, values);
        assert values.size() == 2;

    }

    @Test
    public void dropTable() {
        dropTable(tableName);
    }

}