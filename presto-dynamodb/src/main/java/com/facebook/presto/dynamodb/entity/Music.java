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

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBDocument;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import java.util.List;
import java.util.Set;

/**
 * a sample entity just for test
 *
 * @author songhaifeng
 */
@DynamoDBTable(tableName = "t_Music")
public class Music {
    private static final transient String FYEAR_ARTIST_INDEX = "fyear_artist_index";
    private String artist;
    private String title;
    private String album;
    private int fyear;
    private List<String> strList;
    private Set<Integer> intSet;
    private List<Desc> desces;

    public Music() {
    }

    public Music(String artist, String title, String album, int fyear) {
        this.album = album;
        this.artist = artist;
        this.title = title;
        this.fyear = fyear;
    }

    public Music(String artist, String title, String album, int fyear,
                 List<String> strList, Set<Integer> intSet, List<Desc> desces) {
        this.album = album;
        this.artist = artist;
        this.title = title;
        this.fyear = fyear;
        this.strList = strList;
        this.intSet = intSet;
        this.desces = desces;
    }

    @DynamoDBHashKey(attributeName = "artist")
    @DynamoDBIndexRangeKey(globalSecondaryIndexName = FYEAR_ARTIST_INDEX, attributeName = "artist")
    public String getArtist() {
        return artist;
    }

    public void setArtist(String artist) {
        this.artist = artist;
    }

    @DynamoDBRangeKey(attributeName = "title")
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @DynamoDBAttribute(attributeName = "album")
    public String getAlbum() {
        return album;
    }

    public void setAlbum(String album) {
        this.album = album;
    }

    @DynamoDBAttribute(attributeName = "fYear")
    @DynamoDBIndexHashKey(globalSecondaryIndexName = FYEAR_ARTIST_INDEX, attributeName = "fYear")
    public int getFyear() {
        return fyear;
    }

    public void setFyear(int fyear) {
        this.fyear = fyear;
    }

    @DynamoDBAttribute(attributeName = "str_list")
    public List<String> getStrList() {
        return strList;
    }

    public void setStrList(List<String> strList) {
        this.strList = strList;
    }

    @DynamoDBAttribute(attributeName = "int_set")
    public Set<Integer> getIntSet() {
        return intSet;
    }

    public void setIntSet(Set<Integer> intSet) {
        this.intSet = intSet;
    }

    public List<Desc> getDesces() {
        return desces;
    }

    public void setDesces(List<Desc> desces) {
        this.desces = desces;
    }

    @DynamoDBDocument
    public static class Desc {
        private String comment;

        public Desc() {
        }

        public Desc(String comment) {
            this.comment = comment;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }
    }
}