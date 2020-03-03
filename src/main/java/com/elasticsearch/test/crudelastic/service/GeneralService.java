package com.elasticsearch.test.crudelastic.service;

import com.elasticsearch.test.crudelastic.repository.EsRepository;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service
public class GeneralService {

    private final EsRepository esRepository;

    @Autowired
    public GeneralService(EsRepository esRepository) {
        this.esRepository = esRepository;
    }

    public List<String> getIndexes() {
        GetIndexRequest request = new GetIndexRequest("*");
        GetIndexResponse response = esRepository.indexes(request);
        String[] indices = response.getIndices();
        return Arrays.asList(indices);
    }

    public AcknowledgedResponse deleteIndex(String index){
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
        return esRepository.deleteIndex(deleteIndexRequest);
    }

}
