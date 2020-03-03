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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

@Service
public class CSVEntityService {

    private static final String INDEX = "newcsventity";
    private static final String LAST_CLICKED_MIN = "lastClickedMin";
    private static final String COUNT = "count";
    private final EsRepository esRepository;

    @Autowired
    public CSVEntityService(EsRepository esRepository) {
        this.esRepository = esRepository;
    }

    public GetResponse getCSVEntityById(String id) {
        GetRequest getRequest = new GetRequest(INDEX, id);
        return esRepository.get(getRequest);
    }

    public SearchResponse getCSVEntities(Integer limit) {
        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        if(!Objects.isNull(limit)){
            searchSourceBuilder.size(limit);
        }
        else {
            searchSourceBuilder.size(25);
        }
        searchRequest.source(searchSourceBuilder);
        return esRepository.search(searchRequest);
    }

    public SearchResponse getCSVEntitiesByFieldWithMatchQuery(String field, String value) {
        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(field, value));
        searchRequest.source(searchSourceBuilder);
        return esRepository.search(searchRequest);
    }

    public SearchResponse getCSVEntitiesByFieldWithTermQuery(String field, String value) {//exact match only to field that haven't "text" definition
        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery(field, value));
        searchRequest.source(searchSourceBuilder);
        return esRepository.search(searchRequest);
    }

    public SearchResponse getMostPopularCSVEntities(LocalDateTime startDate, LocalDateTime endDate, Long minCounter) {
        SearchRequest searchRequest = new SearchRequest(INDEX);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.sort(new FieldSortBuilder(COUNT).order(SortOrder.DESC));

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.should(QueryBuilders.rangeQuery(LAST_CLICKED_MIN).from(startDate).to(endDate));

        if(!Objects.isNull(minCounter)){
            boolQuery.filter(QueryBuilders.rangeQuery(COUNT).gt(minCounter));
        }

        searchSourceBuilder.query(boolQuery);
        searchRequest.source(searchSourceBuilder);

        return esRepository.search(searchRequest);
    }

    public String updateCSVEntity(CSVEntity csvEntity) {//todo
        return null;
    }

    public DeleteResponse deleteCSVEntityById(String id) {
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
//https://dzone.com/articles/23-useful-elasticsearch-example-queries
