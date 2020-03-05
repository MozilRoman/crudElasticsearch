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
import org.elasticsearch.index.query.ScriptQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketSelectorPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.SumBucketPipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    /**
     Select csventity.vendorNewId, SUM(count) as amount_of_count
     from csventity
     where (csventity.lastClickedMin BETWEEN startDateFromUser AND endDateFromUser) and amount_of_count >= inputCounterFromUser
     group by csventity.vendorNewId
     order by amount_of_count DESC
     */
    //vendorId = nGLF4dN4Fq201910310GLFILE
    public SearchResponse getMostPopularCSVEntities(LocalDateTime startDate, LocalDateTime endDate, Long minCounter) {
        SearchRequest searchRequest = new SearchRequest(INDEX);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.sort(new FieldSortBuilder(COUNT).order(SortOrder.DESC));

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.rangeQuery(LAST_CLICKED_MIN).from(startDate).to(endDate));
//        boolQuery.must(QueryBuilders.matchQuery("vendorId", "nGLF4dN4Fq201910310GLFILE"));

        if(!Objects.isNull(minCounter)){
            boolQuery.filter(QueryBuilders.rangeQuery(COUNT).gt(minCounter));
        }

        TermsAggregationBuilder sumCounterAggregation = AggregationBuilders
                .terms("sum_counts")
                .field("vendorId");

        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders
                .sum("total_counter")
                .field("count");
        sumCounterAggregation.subAggregation(sumAggregationBuilder);
        sumCounterAggregation.order(BucketOrder.aggregation("total_counter", false));


        Map<String, String> bucketsPathsMap = new HashMap<>();
        bucketsPathsMap.put("target_field", "total_counter");
        Script script = new Script("params.target_field > 2109");
        BucketSelectorPipelineAggregationBuilder bucketSelectorPipelineAggregationBuilder = PipelineAggregatorBuilders.bucketSelector("amount_spent_filter", bucketsPathsMap, script);
        sumCounterAggregation.subAggregation(bucketSelectorPipelineAggregationBuilder);//todo add sorting by total_counter

        searchSourceBuilder.query(boolQuery);
        searchSourceBuilder.aggregation(sumCounterAggregation);
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
//https://www.codota.com/code/java/classes/org.elasticsearch.index.query.QueryBuilders -> bool query
//https://stackoverflow.com/questions/30467211/elastic-search-sum-aggregation-with-group-by-and-where-condition/30467878 -> gropu by
//https://www.reddit.com/r/elasticsearch/comments/9ygcmt/can_i_sort_by_aggregated_value_in_multi_bucket/ -> bucket path
