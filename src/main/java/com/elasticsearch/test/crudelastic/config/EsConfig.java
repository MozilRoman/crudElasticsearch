package com.elasticsearch.test.crudelastic.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Configuration
public class EsConfig {

    private final String elasticsearchHost = "http://localhost:9200";

    @Bean
    public RestHighLevelClient adjustExClient() {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(HttpHost.create(elasticsearchHost)));

        return client;
    }
}
