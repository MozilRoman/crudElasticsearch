package com.elasticsearch.test.crudelastic.repository;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.List;

public class GeneralRepository {

    private final EsClient esClient;

    @Autowired
    public GeneralRepository(EsClient esClient) {
        this.esClient = esClient;
    }

    public List<String> getIndexes() {
        GetIndexRequest request = new GetIndexRequest("*");
        GetIndexResponse response = esClient.indexes(request);
        String[] indices = response.getIndices();
        return Arrays.asList(indices);
    }

    public AcknowledgedResponse deleteIndex(String index){
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
        return esClient.deleteIndex(deleteIndexRequest);
    }

}
