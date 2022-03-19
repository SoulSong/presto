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
package com.facebook.presto.dynamodb.entity;

import java.util.List;

public class TableInfo {
        private String lowercaseTableName;
        private String tableName;
        private List<FiledInfo> fields;
        private List<IndexInfo> indexInfos;
        private List<String> primaryKeys;

        public String getLowercaseTableName() {
                return lowercaseTableName;
        }

        public void setLowercaseTableName(String lowercaseTableName) {
                this.lowercaseTableName = lowercaseTableName;
        }

        public String getTableName() {
                return tableName;
        }

        public void setTableName(String tableName) {
                this.tableName = tableName;
        }

        public List<FiledInfo> getFields() {
                return fields;
        }

        public void setFields(List<FiledInfo> fields) {
                this.fields = fields;
        }

        public List<IndexInfo> getIndexInfos() {
                return indexInfos;
        }

        public void setIndexInfos(List<IndexInfo> indexInfos) {
                this.indexInfos = indexInfos;
        }

        public List<String> getPrimaryKeys() {
                return primaryKeys;
        }

        public void setPrimaryKeys(List<String> primaryKeys) {
                this.primaryKeys = primaryKeys;
        }
}
