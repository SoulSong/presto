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

import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.facebook.airlift.configuration.Config;
import org.apache.commons.lang.StringUtils;

import java.util.LinkedList;
import java.util.List;

/**
 * @author songhaifeng
 */
public class DynamoDbConfig {
    private String accessKey;
    private String secretKey;
    private String region;
    private String endpoint;
    private int limit = 10;
    private int scanLimit = 20;
    // only support jar or http
    private String schemaRegisterType = StringUtils.EMPTY;
    private String packageNames;
    private int refreshSeconds = 0;
    private String schemaUri;

    public static final String JAR = "jar";
    public static final String HTTP = "http";

    @Config("aws.access-key")
    public DynamoDbConfig setAccessKey(String accessKey) {
        this.accessKey = accessKey;
        return this;
    }

    public String getAccessKey() {
        return accessKey;
    }

    @Config("aws.region")
    public DynamoDbConfig setRegion(String region) {
        this.region = region;
        return this;
    }

    public String getRegion() {
        return region;
    }

    @Config("aws.secret-key")
    public DynamoDbConfig setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }

    public String getSecretKey() {
        return secretKey;
    }

    @Config("aws.endpoint")
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getEndpoint() {
        return endpoint;
    }

    @Config("scan.package-names")
    public void setPackageNames(String packageNames) {
        this.packageNames = packageNames;
    }

    public String getPackageNames() {
        return packageNames;
    }

    public int getLimit() {
        return limit;
    }

    @Config("dynamodb.request.limit")
    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getScanLimit() {
        return scanLimit;
    }

    @Config("dynamodb.scan.limit")
    public void setScanLimit(int scanLimit) {
        this.scanLimit = scanLimit;
    }

    public String getSchemaRegisterType() {
        return schemaRegisterType;
    }

    @Config("dynamodb.schema.register.type")
    public void setSchemaRegisterType(String schemaRegisterType) {
        this.schemaRegisterType = schemaRegisterType;
    }

    public int getRefreshSeconds() {
        return refreshSeconds;
    }

    @Config("dynamodb.schema.auto.refresh")
    public void setRefreshSeconds(int refreshSeconds) {
        this.refreshSeconds = refreshSeconds;
    }

    public String getSchemaUri() {
        return schemaUri;
    }

    @Config("dynamodb.schema.uri")
    public void setSchemaUri(String schemaUri) {
        this.schemaUri = schemaUri;
    }

    public AWSCredentialsProvider getCredentials() {
        List<AWSCredentialsProvider> providers = new LinkedList<>();
        if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
            providers.add(new AWSStaticCredentialsProvider(new BasicAWSCredentials(getAccessKey(), getSecretKey())));
        }
        providers.add(new EnvironmentVariableCredentialsProvider());
        providers.add(new SystemPropertiesCredentialsProvider());
        providers.add(new ProfileCredentialsProvider());
        providers.add(WebIdentityTokenCredentialsProvider.create());
        providers.add(new EC2ContainerCredentialsProviderWrapper());
        return new AWSCredentialsProviderChain(providers);
    }

}
