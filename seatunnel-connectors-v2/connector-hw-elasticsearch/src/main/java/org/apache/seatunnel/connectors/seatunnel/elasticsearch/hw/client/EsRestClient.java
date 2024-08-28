/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.client;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.TextNode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.config.EsClusterConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.config.HwEsAuthConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.dto.BulkResponse;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.dto.ElasticsearchClusterInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.dto.source.IndexDocsCount;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.dto.source.ScrollResult;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.exception.ElasticsearchConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.exception.ElasticsearchConnectorException;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.util.LoginUtil;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.Asserts;
import org.apache.http.util.EntityUtils;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.client.EsType.AGGREGATE_METRIC_DOUBLE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.client.EsType.ALIAS;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.client.EsType.DATE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.client.EsType.DATE_NANOS;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.client.EsType.DENSE_VECTOR;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.client.EsType.OBJECT;

@Slf4j
public class EsRestClient {

    private static final int CONNECTION_REQUEST_TIMEOUT = 10 * 1000;

    private static final int SOCKET_TIMEOUT = 5 * 60 * 1000;

    private final RestClient restClient;

    private EsRestClient(RestClient restClient) {
        this.restClient = restClient;
    }

    public static EsRestClient createInstance(ReadonlyConfig pluginConfig) {
        List<String> hosts = pluginConfig.get(EsClusterConnectionConfig.HOSTS);
        Map<String, String> hwEsAuth =
                pluginConfig.get(EsClusterConnectionConfig.HW_ES_AUTH_CONFIG);
        HwEsAuthConfig hwEsAuthConfig = new HwEsAuthConfig(hwEsAuth);
        return createInstance(hosts, hwEsAuthConfig);
    }

    public static EsRestClient createInstance(List<String> hosts, HwEsAuthConfig hwEsAuthConfig) {
        RestClientBuilder restClientBuilder = getRestClientBuilder(hosts, hwEsAuthConfig);
        return new EsRestClient(restClientBuilder.build());
    }

    private static RestClientBuilder getRestClientBuilder(
            List<String> hosts, HwEsAuthConfig hwEsAuthConfig) {
        HttpHost[] httpHosts = getHostArray(hosts);

        if ("false".equals(hwEsAuthConfig.getIsSecureMode())) {
            System.setProperty("es.security.indication", "false");
        } else {
            try {
                LoginUtil.setJaasFile(
                        hwEsAuthConfig.getPrincipal(),
                        hwEsAuthConfig.getKeytab(),
                        hwEsAuthConfig.getCustomJaasPath());
                LoginUtil.setKrb5Config(hwEsAuthConfig.getKrb5Path());
                System.setProperty("elasticsearch.kerberos.jaas.appname", "EsClient");
                System.setProperty("es.security.indication", "true");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        RestClientBuilder builder = RestClient.builder(httpHosts);
        Header[] defaultHeaders =
                new Header[] {
                    new BasicHeader("Accept", "application/json"),
                    new BasicHeader("Content-type", "application/json")
                };
        builder.setRequestConfigCallback(
                        requestConfigBuilder ->
                                requestConfigBuilder
                                        .setConnectTimeout(hwEsAuthConfig.getConnectTimeout())
                                        .setSocketTimeout(hwEsAuthConfig.getSocketTimeout())
                                        .setConnectionRequestTimeout(
                                                hwEsAuthConfig.getConnectionRequestTimeout()))
                .setMaxConnPerRoute(hwEsAuthConfig.getMaxConnPerRoute())
                .setMaxConnTotal(hwEsAuthConfig.getMaxConnTotal());
        builder.setDefaultHeaders(defaultHeaders);
        return builder;
    }

    private static HttpHost[] getHostArray(List<String> hostsStr) {
        List<HttpHost> hosts = new ArrayList<>();
        for (String host : hostsStr) {
            hosts.add(HttpHost.create(host));
        }

        return hosts.toArray(new HttpHost[0]);
    }

    public BulkResponse bulk(String requestBody) {
        Request request = new Request("POST", "/_bulk");
        request.setJsonEntity(requestBody);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                        "bulk es Response is null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                ObjectMapper objectMapper = new ObjectMapper();
                String entity = EntityUtils.toString(response.getEntity());
                JsonNode json = objectMapper.readTree(entity);
                int took = json.get("took").asInt();
                boolean errors = json.get("errors").asBoolean();
                return new BulkResponse(errors, took, entity);
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                        String.format(
                                "bulk es response status code=%d,request boy=%s",
                                response.getStatusLine().getStatusCode(), requestBody));
            }
        } catch (IOException e) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                    String.format("bulk es error,request boy=%s", requestBody),
                    e);
        }
    }

    public ElasticsearchClusterInfo getClusterInfo() {
        Request request = new Request("GET", "/");
        try {
            Response response = restClient.performRequest(request);
            String result = EntityUtils.toString(response.getEntity());
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(result);
            JsonNode versionNode = jsonNode.get("version");
            return ElasticsearchClusterInfo.builder()
                    .clusterVersion(versionNode.get("number").asText())
                    .distribution(
                            Optional.ofNullable(versionNode.get("distribution"))
                                    .map(JsonNode::asText)
                                    .orElse(null))
                    .build();
        } catch (IOException e) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.GET_ES_VERSION_FAILED,
                    "fail to get elasticsearch version.",
                    e);
        }
    }

    public void close() {
        try {
            restClient.close();
        } catch (IOException e) {
            log.warn("close elasticsearch connection error", e);
        }
    }

    /**
     * first time to request search documents by scroll call /${index}/_search?scroll=${scroll}
     *
     * @param index index name
     * @param source select fields
     * @param scrollTime such as:1m
     * @param scrollSize fetch documents count in one request
     */
    public ScrollResult searchByScroll(
            String index,
            List<String> source,
            Map<String, Object> query,
            String scrollTime,
            int scrollSize) {
        Map<String, Object> param = new HashMap<>();
        param.put("query", query);
        param.put("_source", source);
        param.put("sort", new String[] {"_doc"});
        param.put("size", scrollSize);
        String endpoint = "/" + index + "/_search?scroll=" + scrollTime;
        return getDocsFromScrollRequest(endpoint, JsonUtils.toJsonString(param));
    }

    /**
     * scroll to get result call _search/scroll
     *
     * @param scrollId the scroll id of the last request
     * @param scrollTime such as:1m
     */
    public ScrollResult searchWithScrollId(String scrollId, String scrollTime) {
        Map<String, String> param = new HashMap<>();
        param.put("scroll_id", scrollId);
        param.put("scroll", scrollTime);
        return getDocsFromScrollRequest("/_search/scroll", JsonUtils.toJsonString(param));
    }

    private ScrollResult getDocsFromScrollRequest(String endpoint, String requestBody) {
        Request request = new Request("POST", endpoint);
        request.setJsonEntity(requestBody);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                        "POST " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                ObjectNode responseJson = JsonUtils.parseObject(entity);

                JsonNode shards = responseJson.get("_shards");
                int totalShards = shards.get("total").intValue();
                int successful = shards.get("successful").intValue();
                Asserts.check(
                        totalShards == successful,
                        String.format(
                                "POST %s,total shards(%d)!= successful shards(%d)",
                                endpoint, totalShards, successful));

                return getDocsFromScrollResponse(responseJson);
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                        String.format(
                                "POST %s response status code=%d,request boy=%s",
                                endpoint, response.getStatusLine().getStatusCode(), requestBody));
            }
        } catch (IOException e) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                    String.format("POST %s error,request boy=%s", endpoint, requestBody),
                    e);
        }
    }

    private ScrollResult getDocsFromScrollResponse(ObjectNode responseJson) {
        ScrollResult scrollResult = new ScrollResult();
        String scrollId = responseJson.get("_scroll_id").asText();
        scrollResult.setScrollId(scrollId);

        JsonNode hitsNode = responseJson.get("hits").get("hits");
        List<Map<String, Object>> docs = new ArrayList<>(hitsNode.size());
        scrollResult.setDocs(docs);

        for (JsonNode jsonNode : hitsNode) {
            Map<String, Object> doc = new HashMap<>();
            doc.put("_index", jsonNode.get("_index").textValue());
            doc.put("_id", jsonNode.get("_id").textValue());
            JsonNode source = jsonNode.get("_source");
            for (Iterator<Map.Entry<String, JsonNode>> iterator = source.fields();
                    iterator.hasNext(); ) {
                Map.Entry<String, JsonNode> entry = iterator.next();
                String fieldName = entry.getKey();
                if (entry.getValue() instanceof TextNode) {
                    doc.put(fieldName, entry.getValue().textValue());
                } else {
                    doc.put(fieldName, entry.getValue());
                }
            }
            docs.add(doc);
        }
        return scrollResult;
    }

    public List<IndexDocsCount> getIndexDocsCount(String index) {
        String endpoint = String.format("/_cat/indices/%s?h=index,docsCount&format=json", index);
        Request request = new Request("GET", endpoint);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                        "GET " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                return JsonUtils.toList(entity, IndexDocsCount.class);
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                        String.format(
                                "GET %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException ex) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED, ex);
        }
    }

    public List<String> listIndex() {
        String endpoint = "/_cat/indices?format=json";
        Request request = new Request("GET", endpoint);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.LIST_INDEX_FAILED,
                        "GET " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                return JsonUtils.toList(entity, Map.class).stream()
                        .map(map -> map.get("index").toString())
                        .collect(Collectors.toList());
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.LIST_INDEX_FAILED,
                        String.format(
                                "GET %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException ex) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.LIST_INDEX_FAILED, ex);
        }
    }

    public void createIndex(String indexName) {
        createIndex(indexName, null);
    }

    public void createIndex(String indexName, String mapping) {
        String endpoint = String.format("/%s", indexName);
        Request request = new Request("PUT", endpoint);
        if (StringUtils.isNotEmpty(mapping)) {
            request.setJsonEntity(mapping);
        }
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.CREATE_INDEX_FAILED,
                        "PUT " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.CREATE_INDEX_FAILED,
                        String.format(
                                "PUT %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException ex) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.CREATE_INDEX_FAILED, ex);
        }
    }

    public void dropIndex(String tableName) {
        String endpoint = String.format("/%s", tableName);
        Request request = new Request("DELETE", endpoint);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.DROP_INDEX_FAILED,
                        "DELETE " + endpoint + " response null");
            }
            // todo: if the index doesn't exist, the response status code is 200?
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                return;
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.DROP_INDEX_FAILED,
                        String.format(
                                "DELETE %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException ex) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.DROP_INDEX_FAILED, ex);
        }
    }

    public void clearIndexData(String indexName) {
        String endpoint = String.format("/%s/_delete_by_query", indexName);
        Request request = new Request("POST", endpoint);
        String jsonString = "{ \"query\": { \"match_all\": {} } }";
        request.setJsonEntity(jsonString);

        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.CLEAR_INDEX_DATA_FAILED,
                        "POST " + endpoint + " response null");
            }
            // todo: if the index doesn't exist, the response status code is 200?
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                return;
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.CLEAR_INDEX_DATA_FAILED,
                        String.format(
                                "POST %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException ex) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.CLEAR_INDEX_DATA_FAILED, ex);
        }
    }

    /**
     * get es field name and type mapping realtion
     *
     * @param index index name
     * @return {key-> field name,value->es type}
     */
    public Map<String, BasicTypeDefine<EsType>> getFieldTypeMapping(
            String index, List<String> source) {
        String endpoint = String.format("/%s/_mappings", index);
        Request request = new Request("GET", endpoint);
        Map<String, BasicTypeDefine<EsType>> mapping = new HashMap<>();
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                        "GET " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                        String.format(
                                "GET %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
            String entity = EntityUtils.toString(response.getEntity());
            log.info(String.format("GET %s respnse=%s", endpoint, entity));
            ObjectNode responseJson = JsonUtils.parseObject(entity);
            for (Iterator<JsonNode> it = responseJson.elements(); it.hasNext(); ) {
                JsonNode indexProperty = it.next();
                JsonNode mappingsProperty = indexProperty.get("mappings");
                if (mappingsProperty.has("mappingsProperty")) {
                    JsonNode properties = mappingsProperty.get("properties");
                    mapping = getFieldTypeMappingFromProperties(properties, source);
                } else {
                    for (JsonNode typeNode : mappingsProperty) {
                        JsonNode properties;
                        if (typeNode.has("properties")) {
                            properties = typeNode.get("properties");
                        } else {
                            properties = typeNode;
                        }
                        mapping.putAll(getFieldTypeMappingFromProperties(properties, source));
                    }
                }
            }
        } catch (IOException ex) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED, ex);
        }
        return mapping;
    }

    private static Map<String, BasicTypeDefine<EsType>> getFieldTypeMappingFromProperties(
            JsonNode properties, List<String> source) {
        Map<String, BasicTypeDefine<EsType>> allElasticSearchFieldTypeInfoMap = new HashMap<>();
        properties
                .fields()
                .forEachRemaining(
                        entry -> {
                            String fieldName = entry.getKey();
                            JsonNode fieldProperty = entry.getValue();
                            if (fieldProperty.has("type")) {
                                String type = fieldProperty.get("type").asText();
                                BasicTypeDefine.BasicTypeDefineBuilder<EsType> typeDefine =
                                        BasicTypeDefine.<EsType>builder()
                                                .name(fieldName)
                                                .columnType(type)
                                                .dataType(type);
                                if (type.equalsIgnoreCase(AGGREGATE_METRIC_DOUBLE)) {
                                    ArrayNode metrics = ((ArrayNode) fieldProperty.get("metrics"));
                                    List<String> metricsList = new ArrayList<>();
                                    for (JsonNode node : metrics) {
                                        metricsList.add(node.asText());
                                    }
                                    Map<String, Object> options = new HashMap<>();
                                    options.put("metrics", metricsList);
                                    typeDefine.nativeType(new EsType(type, options));
                                } else if (type.equalsIgnoreCase(ALIAS)) {
                                    String path = fieldProperty.get("path").asText();
                                    Map<String, Object> options = new HashMap<>();
                                    options.put("path", path);
                                    typeDefine.nativeType(new EsType(type, options));
                                } else if (type.equalsIgnoreCase(DENSE_VECTOR)) {
                                    String elementType =
                                            fieldProperty.get("element_type") == null
                                                    ? "float"
                                                    : fieldProperty.get("element_type").asText();
                                    Map<String, Object> options = new HashMap<>();
                                    options.put("element_type", elementType);
                                    typeDefine.nativeType(new EsType(type, options));
                                } else if (type.equalsIgnoreCase(DATE)
                                        || type.equalsIgnoreCase(DATE_NANOS)) {
                                    String format =
                                            fieldProperty.get("format") != null
                                                    ? fieldProperty.get("format").asText()
                                                    : "strict_date_optional_time_nanos||epoch_millis";
                                    Map<String, Object> options = new HashMap<>();
                                    options.put("format", format);
                                    typeDefine.nativeType(new EsType(type, options));
                                } else {
                                    typeDefine.nativeType(new EsType(type, new HashMap<>()));
                                }
                                allElasticSearchFieldTypeInfoMap.put(fieldName, typeDefine.build());
                            } else if (fieldProperty.has("properties")) {
                                // it should be object type
                                JsonNode propertiesNode = fieldProperty.get("properties");
                                List<String> fields = new ArrayList<>();
                                propertiesNode.fieldNames().forEachRemaining(fields::add);
                                Map<String, BasicTypeDefine<EsType>> subFieldTypeInfoMap =
                                        getFieldTypeMappingFromProperties(propertiesNode, fields);
                                BasicTypeDefine.BasicTypeDefineBuilder<EsType> typeDefine =
                                        BasicTypeDefine.<EsType>builder()
                                                .name(fieldName)
                                                .columnType(OBJECT)
                                                .dataType(OBJECT);
                                typeDefine.nativeType(
                                        new EsType(OBJECT, (Map) subFieldTypeInfoMap));
                                allElasticSearchFieldTypeInfoMap.put(fieldName, typeDefine.build());
                            }
                        });
        if (CollectionUtils.isEmpty(source)) {
            return allElasticSearchFieldTypeInfoMap;
        }

        allElasticSearchFieldTypeInfoMap.forEach(
                (fieldName, fieldType) -> {
                    if (fieldType.getDataType().equalsIgnoreCase(ALIAS)) {
                        BasicTypeDefine<EsType> type =
                                allElasticSearchFieldTypeInfoMap.get(
                                        fieldType.getNativeType().getOptions().get("path"));
                        if (type != null) {
                            allElasticSearchFieldTypeInfoMap.put(fieldName, type);
                        }
                    }
                });

        return source.stream()
                .collect(
                        Collectors.toMap(
                                Function.identity(),
                                fieldName -> {
                                    BasicTypeDefine<EsType> fieldType =
                                            allElasticSearchFieldTypeInfoMap.get(fieldName);
                                    if (fieldType == null) {
                                        log.warn(
                                                "fail to get elasticsearch field {} mapping type,so give a default type text",
                                                fieldName);
                                        return BasicTypeDefine.<EsType>builder()
                                                .name(fieldName)
                                                .columnType("text")
                                                .dataType("text")
                                                .build();
                                    }
                                    return fieldType;
                                }));
    }
}
