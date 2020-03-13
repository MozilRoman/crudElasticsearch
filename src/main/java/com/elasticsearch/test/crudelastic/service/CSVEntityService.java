package com.elasticsearch.test.crudelastic.service;

import com.elasticsearch.test.crudelastic.repository.CSVEntityRepository;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;

@Service
public class CSVEntityService {

    private final CSVEntityRepository csvEntityRepository;

    @Autowired
    public CSVEntityService(CSVEntityRepository csvEntityRepository) {
        this.csvEntityRepository = csvEntityRepository;
    }

    public GetResponse getCSVEntityById(String id) {
        return csvEntityRepository.getCSVEntityById(id);
    }

    public SearchResponse getCSVEntities(Integer limit) {
        return csvEntityRepository.getCSVEntities(limit);
    }

    public SearchResponse getCSVEntitiesByFieldWithMatchQuery(String field, String value) {
        return csvEntityRepository.getCSVEntitiesByFieldWithMatchQuery(field, value);
    }

    public SearchResponse getCSVEntitiesByFieldWithTermQuery(String field, String value) {
        return csvEntityRepository.getCSVEntitiesByFieldWithTermQuery(field, value);
    }

    public SearchResponse getMostPopularCSVEntities(LocalDateTime startDate, LocalDateTime endDate, Long minCounter) {
        return csvEntityRepository.getMostPopularCSVEntities(startDate, endDate, minCounter);
    }

    public UpdateResponse updateCSVEntityById(String field, String newValue, String id) {
        return csvEntityRepository.updateCSVEntityById(field, newValue, id);
    }

    public BulkByScrollResponse updateCSVEntitiesByQuery(String field, String oldValue, String newValue) {
        return csvEntityRepository.updateCSVEntitiesByQuery(field, oldValue, newValue);
    }

    public DeleteResponse deleteCSVEntityById(String id) {
        return csvEntityRepository.deleteCSVEntityById(id);
    }

    public BulkResponse bulkIngestion(List<String> documents) {
        return csvEntityRepository.bulkIngestion(documents);
    }
}
