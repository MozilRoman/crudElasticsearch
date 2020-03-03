package com.elasticsearch.test.crudelastic.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EsConfig {

    private static final String ELASTICSEARCH_HOST = "http://localhost:9200";

    @Bean
    public RestHighLevelClient adjustExClient() {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(HttpHost.create(ELASTICSEARCH_HOST)));

        return client;
    }
}
