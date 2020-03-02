package com.elasticsearch.test.crudelastic.service;

import com.elasticsearch.test.crudelastic.entity.CSVEntity;
import com.elasticsearch.test.crudelastic.repository.EsRepository;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service
public class CSVEntityService {

    private static final String INDEX = "csventity1";
    private final EsRepository esRepository;

    @Autowired
    public CSVEntityService(EsRepository esRepository) {
        this.esRepository = esRepository;
    }

    public GetResponse getById(String id) {
        GetRequest getRequest = new GetRequest(INDEX, id);
        return esRepository.get(getRequest);
    }

    public SearchResponse getByFieldWithMatchQuery(String field, String value) {
        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(field, value));
        searchRequest.source(searchSourceBuilder);
        return esRepository.search(searchRequest);
    }

    public SearchResponse getByFieldWithTermQuery(String field, String value) {//exact match only to field that haven't "text" definition
        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery(field, value));
        searchRequest.source(searchSourceBuilder);
        return esRepository.search(searchRequest);
    }

    public String updateCSVEntity(CSVEntity csvEntity) {//todo
        return null;
    }

    public DeleteResponse deleteById(String id) {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX, id);
        return esRepository.delete(deleteRequest);
    }

    public BulkResponse bulkIngestion(List<String> documents) {
        BulkRequest bulkRequest = new BulkRequest();
        documents.forEach(csvEntity -> {
            IndexRequest indexRequest = new IndexRequest(INDEX)
                    .source(csvEntity, XContentType.JSON);
            bulkRequest.add(indexRequest);
        });
        //todo add RefreshPolicy

        return esRepository.bulk(bulkRequest);
    }
}
