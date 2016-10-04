package com.qubole.streamx.hive;

import io.confluent.connect.hdfs.errors.HiveMetaStoreException;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.reflect.Type;
import java.lang.Object;
import java.util.Map;
import java.util.List;
import java.io.IOException;

public class JsonRestClient {
    private Gson gsonConverter;
    private String authToken;
    private static final Logger log = LoggerFactory.getLogger(JsonRestClient.class);

    public JsonRestClient(String token) {
        // Instantiate Gson Converter
        gsonConverter = new Gson();
        authToken = token;
    }

    // Helper function to extract data from JSON String
    public <T> T parseResults(String jsonString, Type returnType) {
        log.info("Parsing Request results");
        log.debug("JSON: " + jsonString);
        return gsonConverter.fromJson(jsonString, returnType);
    }

    // All GET requests route through here; canonical httpClient example
    public Map getRequest(String url) throws IOException {
        try {
            // Create HTTP GET request for JSON requests
            log.info("GET Request: Endpoint: " + url);
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpGet httpGet = new HttpGet(url);
            httpGet.addHeader("X-AUTH-TOKEN", authToken);
            httpGet.addHeader("Content-Type", "application/json");
            httpGet.addHeader("Accept", "application/json");
            String result;

            CloseableHttpResponse httpResponse = httpClient.execute(httpGet);
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            log.info("GET Status Code: {}", statusCode);
            if(statusCode != HttpStatus.SC_OK) {
                log.error(httpResponse.getStatusLine().getReasonPhrase());
                throw new RuntimeException("HTTP Request Failed: " + statusCode);
            }
            result = EntityUtils.toString(httpResponse.getEntity());
            httpResponse.close();
            return parseResults(result, new TypeToken<Map<String, Object>>() {}.getType());
        } catch (IOException | RuntimeException e) {
            throw new HiveMetaStoreException(e);
        }
    }

    public Map postRequest(String url, Map queryMap) throws IOException {
        try {
            // Create HTTP POST request for JSON requests
            log.info("POST Request: Endpoint: " + url);
            String jsonString = gsonConverter.toJson(queryMap);
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost(url);
            httpPost.addHeader("X-AUTH-TOKEN", authToken);
            httpPost.addHeader("Content-Type", "application/json");
            httpPost.addHeader("Accept", "application/json");

            StringEntity payload = new StringEntity(jsonString);
            httpPost.setEntity(payload);
            CloseableHttpResponse httpResponse = httpClient.execute(httpPost);
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            if(statusCode != HttpStatus.SC_OK) {
                log.error(httpResponse.getStatusLine().getReasonPhrase());
                throw new RuntimeException("HTTP Request Failed: " + statusCode);
            }

            String result = EntityUtils.toString(httpResponse.getEntity());
            log.info("Response: {}", result);
            httpResponse.close();
            return parseResults(result, new TypeToken<Map<String, Object>>() {}.getType());

        } catch (IOException | RuntimeException e) {
            throw new HiveMetaStoreException(e);
        }

    }
}
