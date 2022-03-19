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
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.dynamodb.entity.Music;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamoDbComplexSampleTest extends AbstractDynamoDbTest {
    private static final Logger log = Logger.get(DynamoDbComplexSampleTest.class);

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
        Music.Desc desc1 = new Music.Desc("bad");
        Music.Desc desc2 = new Music.Desc("good");
        Music.Desc desc3 = new Music.Desc("perfect");
        Music.Desc desc4 = new Music.Desc("excellent");

        dynamoDbMapper.batchSave(new Music("lin junjie", "caocao", "曹操", 2006, ImmutableList.of("a", "b"), ImmutableSet.of(1, 2), ImmutableList.of(desc1, desc2, desc3))
                , new Music("lin junjie", "jiangnan", "江南", 2004, null, ImmutableSet.of(3, 2), ImmutableList.of(desc2, desc3))
                , new Music("lin junjie", "xijie", null, 2007, ImmutableList.of("a", "b", "c"), null, ImmutableList.of(desc1, desc2, desc3, desc4))
                , new Music("lin junjie", "xindiqiu", "新地球", 2014, ImmutableList.of("d"), ImmutableSet.of(1, 2, 4, 5, 6, 9), ImmutableList.of(desc3))
                , new Music("zhou jielun", "jiangnan", "范特西", 2001, ImmutableList.of("a", "b"), ImmutableSet.of(1, 2), ImmutableList.of(desc4, desc2, desc3))
                , new Music("zhou jielun", "badukongjian", "八度空间", 2002, ImmutableList.of("b"), ImmutableSet.of(1, 2), ImmutableList.of(desc3))
                , new Music("zhou jielun", "jingtanhao", "惊叹号", 2010, ImmutableList.of("e", "b"), ImmutableSet.of(1, 2), null)
                , new Music("zhou jielun", "wohenmang", "我很忙", 2007, null, null, ImmutableList.of(desc1, desc4, desc3))
        );
        log.info("load success.");
    }

    /**
     * 模拟常见的集中查询方式(基于expression)，便于后续基于presto的实现参考
     */
    @Test
    public void query() {
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();

        QueryRequest queryRequest = new QueryRequest()
                .withTableName(tableName)
                .withLimit(1)
                .withProjectionExpression("artist,album,fYear,title,str_list,int_set,desces");

        List<Map<String, AttributeValue>> values = new ArrayList<>();

        // 提供完整的hashKey和rangKey，则直接通过load完成查询
        expressionAttributeValues.put(":artist" + ComparisonOperator.EQ.name(), new AttributeValue().withS("lin junjie"));
        expressionAttributeValues.put(":title" + ComparisonOperator.EQ, new AttributeValue().withS("jiangnan"));
        queryRequest.withKeyConditionExpression("artist = :artist" + ComparisonOperator.EQ.name() + " and title = :title" + ComparisonOperator.EQ)
                .withExpressionAttributeValues(expressionAttributeValues);
        execute(queryRequest, values);
        assert values.size() == 1;
    }

    @Test
    public void dropTable() {
        dropTable(tableName);
    }
}