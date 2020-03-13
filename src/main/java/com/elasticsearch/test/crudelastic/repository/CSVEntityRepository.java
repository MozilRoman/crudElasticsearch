package com.elasticsearch.test.crudelastic.repository;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketSelectorPipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.*;

@Component
public class CSVEntityRepository {

    private final EsClient esClient;

    //    private static final String INDEX = "newcsventity";
    private static final String INDEX = "newcsventity1";
    private static final String LAST_CLICKED_MIN = "lastClickedMin";
    private static final String COUNT = "count";
    private static final String SUM_COUNTS = "sum_counts";
    private static final String VENDOR_ID = "vendorId";
    private static final String TOTAL_COUNTER = "total_counter";

    @Autowired
    public CSVEntityRepository(EsClient esClient) {
        this.esClient = esClient;
    }

    public GetResponse getCSVEntityById(String id) {
        GetRequest getRequest = new GetRequest(INDEX, id);
        return esClient.get(getRequest);
    }

    public SearchResponse getCSVEntities(Integer limit) {
        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        if (!Objects.isNull(limit)) {
            searchSourceBuilder.size(limit);
        } else {
            searchSourceBuilder.size(25);
        }
        searchRequest.source(searchSourceBuilder);
        return esClient.search(searchRequest);
    }

    public SearchResponse getCSVEntitiesByFieldWithMatchQuery(String field, String value) {
        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(field, value));
        searchRequest.source(searchSourceBuilder);
        return esClient.search(searchRequest);
    }

    public SearchResponse getCSVEntitiesByFieldWithTermQuery(String field, String value) {//exact match only to field that haven't "text" definition
        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery(field, value));
        searchRequest.source(searchSourceBuilder);
        return esClient.search(searchRequest);
    }

    /**
     * Select csventity.vendorNewId, SUM(count) as amount_of_count
     * from csventity
     * where (csventity.lastClickedMin BETWEEN startDateFromUser AND endDateFromUser) and amount_of_count >= inputCounterFromUser
     * group by csventity.vendorNewId
     * order by amount_of_count DESC
     */
    //vendorId = nGLF4dN4Fq201910310GLFILE
    public SearchResponse getMostPopularCSVEntities(LocalDateTime startDate, LocalDateTime endDate, Long minCounter) {
        SearchRequest searchRequest = new SearchRequest(INDEX);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.rangeQuery(LAST_CLICKED_MIN).from(startDate).to(endDate));

        TermsAggregationBuilder sumCounterAggregation = AggregationBuilders
                .terms(SUM_COUNTS)
                .field(VENDOR_ID);

        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders
                .sum(TOTAL_COUNTER)
                .field(COUNT);
        sumCounterAggregation.subAggregation(sumAggregationBuilder);
        sumCounterAggregation.order(BucketOrder.aggregation(TOTAL_COUNTER, false));

        if (!Objects.isNull(minCounter)) {
            Map<String, String> bucketsPathsMap = new HashMap<>();
            bucketsPathsMap.put("target_field", TOTAL_COUNTER);
            Script script = new Script("params.target_field > " + minCounter);//minCounter=2109
            BucketSelectorPipelineAggregationBuilder bucketSelectorPipelineAggregationBuilder = PipelineAggregatorBuilders.bucketSelector("amount_spent_filter", bucketsPathsMap, script);
            sumCounterAggregation.subAggregation(bucketSelectorPipelineAggregationBuilder);
        }

        searchSourceBuilder.query(boolQuery);
        searchSourceBuilder.aggregation(sumCounterAggregation);
        searchRequest.source(searchSourceBuilder);

        return esClient.search(searchRequest);
    }

    public UpdateResponse updateCSVEntityById(String field, String newValue, String id) {//need test
        UpdateRequest updateRequest = new UpdateRequest(INDEX, id);
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put(field, newValue);
        updateRequest.doc(jsonMap);
        return esClient.update(updateRequest);
    }

    /**
     * UPDATE csventity
     * SET csventity.vendorId = '1'
     * WHERE csventity.vendorId = 'null';
     */
    public BulkByScrollResponse updateCSVEntitiesByQuery(String field, String oldValue, String newValue) {
        UpdateByQueryRequest request = new UpdateByQueryRequest(INDEX);

        request.setConflicts("proceed");

        //look like: (ctx._source.field == 'oldValue') {ctx._source.field = 'newValue';}
        String query = "if (ctx._source." + field + " == '" + oldValue + "') {ctx._source." + field + " = " + newValue + ";}";

        request.setScript(
                new Script(
                        ScriptType.INLINE, "painless",
                        query,
                        Collections.emptyMap()));// check: maybe we can substitute query to Map

        return esClient.updateByQuery(request);
    }

    public DeleteResponse deleteCSVEntityById(String id) {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX, id);
        return esClient.delete(deleteRequest);
    }

    public BulkResponse bulkIngestion(List<String> documents) {
        BulkRequest bulkRequest = new BulkRequest();
        documents.forEach(csvEntity -> {
            IndexRequest indexRequest = new IndexRequest(INDEX)
                    .source(csvEntity, XContentType.JSON);
            bulkRequest.add(indexRequest);
        });
        //todo add RefreshPolicy

        return esClient.bulk(bulkRequest);
    }

}
//https://dzone.com/articles/23-useful-elasticsearch-example-queries
//https://www.codota.com/code/java/classes/org.elasticsearch.index.query.QueryBuilders -> bool query
//https://stackoverflow.com/questions/30467211/elastic-search-sum-aggregation-with-group-by-and-where-condition/30467878 -> gropu by
//https://www.reddit.com/r/elasticsearch/comments/9ygcmt/can_i_sort_by_aggregated_value_in_multi_bucket/ -> bucket path
