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

import java.util.Set;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/7/6 13:38
 */
public class DynamoDbIndex {
    private String indexName;
    private String hashKey;
    private String rangeKey;
    private Set<String> projections;

    public DynamoDbIndex() {
    }

    public DynamoDbIndex(String indexName, String hashKey, String rangeKey, Set<String> projections) {
        this.indexName = indexName;
        this.hashKey = hashKey;
        this.rangeKey = rangeKey;
        this.projections = projections;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getHashKey() {
        return hashKey;
    }

    public void setHashKey(String hashKey) {
        this.hashKey = hashKey;
    }

    public String getRangeKey() {
        return rangeKey;
    }

    public void setRangeKey(String rangeKey) {
        this.rangeKey = rangeKey;
    }

    public Set<String> getProjections() {
        return projections;
    }

    public void setProjections(Set<String> projections) {
        this.projections = projections;
    }
}
