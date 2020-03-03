package com.elasticsearch.test.crudelastic.mainMethod;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.List;

public class EsClient {

    private final RestHighLevelClient client;

    public EsClient(){
        client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://localhost:9200")));
    }

    public String createIndex (){
        CreateIndexRequest request = new CreateIndexRequest("csventity");

//        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://localhost:9200")));
        client.indices();

//        try (RestHighLevelClient client = new RestHighLevelClient(
//                RestClient.builder(HttpHost.create("http://localhost:9200")))) {
//            try {
//                client.indices().delete(new DeleteIndexRequest("test"), RequestOptions.DEFAULT);
//            } catch (ElasticsearchStatusException ignored) { }
//            client.index(new IndexRequest("test").id("1").source("{\"foo\":\"bar\", \"application_id\": 6}", XContentType.JSON), RequestOptions.DEFAULT);
////            GetResponse getResponse = client.get(new GetRequest("test", "1").fetchSourceContext(
////                    new FetchSourceContext(true, new String[]{"application_id"}, null)
////            ), RequestOptions.DEFAULT);
////            System.out.println("doc = " + getResponse);
//        } catch (Exception e) {
//            e.printStackTrace(System.err);
//        }

        return "";
    }

    public IndexResponse putDocumentInIndex (String document, String index){
//        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://localhost:9200")));

        IndexRequest request = new IndexRequest(index);
        request.id("1");
        request.source(document, XContentType.JSON);

        try{
            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
            SearchResponse searchResponse = searchAllDocumentsInIndex(index);
            return indexResponse;
        }
        catch (Exception e){
            e.printStackTrace();
        }

        return null;
    }

    public BulkResponse bulkIngestionInIndex (List<String> documents, String index){
//        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://localhost:9200")));

        BulkRequest bulkRequest = new BulkRequest();
        documents.forEach(csventity -> {
            IndexRequest indexRequest = new IndexRequest(index)
                    .source(csventity, XContentType.JSON);
            bulkRequest.add(indexRequest);
        });
        //todo add RefreshPolicy

        try{
            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
//            SearchResponse searchResponse = searchAllDocumentsInIndex(index);
            return bulkResponse;
        }
        catch (Exception e){
            e.printStackTrace();
        }

        return null;
    }

    public BulkResponse bulkIngestionWithRefreshPolicyInIndex (List<String> documents, String index){
//        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://localhost:9200")));

        BulkRequest bulkRequest = new BulkRequest();

        // Setting batch value to make requests in batches since our number of rows can be in millions or more than that
        int batch = 1000;
        // Conter to check how many rows have been processed
        int counter = 0;

        documents.forEach(csventity -> {
            IndexRequest indexRequest = new IndexRequest(index)
                    .source(csventity, XContentType.JSON);
            bulkRequest.add(indexRequest);
        });
        //todo add RefreshPolicy

        try{
            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            SearchResponse searchResponse = searchAllDocumentsInIndex(index);
            return bulkResponse;
        }
        catch (Exception e){
            e.printStackTrace();
        }

        return null;
    }

    public String[] getIndexes (){
//        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://localhost:9200")));
        GetIndexRequest request = new GetIndexRequest("*");

        try{
            GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);
            String[] indices = response.getIndices();
            return indices;
        }
        catch (Exception e){
           e.printStackTrace();
        }

        return null;
    }

    public GetResponse getDocumentFromIndexById (String index, String id){
//        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://localhost:9200")));
        GetRequest getRequest = new GetRequest(index, id);

        try{
            GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
            return getResponse;
        }
        catch (Exception e){
            e.printStackTrace();
        }

        return null;
    }

    public SearchResponse searchAllDocumentsInIndex(String index){
//        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://localhost:9200")));
        SearchRequest searchRequest = new SearchRequest(index);
//        SearchRequest searchRequest = new SearchRequest("bank");

        try{
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchRequest.source(searchSourceBuilder);
            SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
            System.out.println();
            return search;
        }
        catch (Exception e){
            e.printStackTrace();
        }

        return null;
    }

    public SearchResponse searchDocumentsFromIndexInFieldByName(String index, String field, String value){
//        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://localhost:9200")));
        SearchRequest searchRequest = new SearchRequest(index);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(field, value));//matchQuery- Returns documents that match a provided text, number, date or boolean value. The provided text is analyzed before matching.
//        searchSourceBuilder.query(QueryBuilders.termQuery(field, value));
//        searchSourceBuilder.query(QueryBuilders.termQuery("sources", "BSW"));// sources : "BSW"  -> search in field "sources" val "BSV"
        // QueryBuilders.matchQuery("sources", "BSW")
        searchSourceBuilder.size(5);//Set the size option that determines the number of search hits to return. Defaults to 10.
//        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));//Sort descending by _score (the default)
//        searchSourceBuilder.sort(new FieldSortBuilder("id").order(SortOrder.ASC));// Also sort ascending by _id field

        searchRequest.source(searchSourceBuilder);

        try{
            SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
            return search;
        }
        catch (Exception e){
            e.printStackTrace();
        }

        return null;
    }

    public SearchResponse searchDocumentsFromIndexInFieldByExactValue(String index, String field, String value){
//        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://localhost:9200")));
        SearchRequest searchRequest = new SearchRequest(index);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery(field, value)); //termQuery - Returns documents that contain an exact term in a provided field
        searchSourceBuilder.size(5);//Set the size option that determines the number of search hits to return. Defaults to 10.

        searchRequest.source(searchSourceBuilder);

        try{
            SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
            return search;
        }
        catch (Exception e){
            e.printStackTrace();
        }

        return null;
        //Because full_text is a text field, Elasticsearch changes Quick Brown Foxes!(during ingestion) to [quick, brown, fox] during analysis.
        //so you should use termQuery only to fields, type of that IS NOT "text"
    }

    public UpdateResponse updateDocumentFromIndexById(String index, String id, String field, String newValue){
//        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://localhost:9200")));
        UpdateRequest updateRequest = new UpdateRequest(index, id).doc(field, newValue);

        try{
            UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
            return updateResponse;
        }
        catch (Exception e){
            e.printStackTrace();
        }

        return null;
        //Because full_text is a text field, Elasticsearch changes Quick Brown Foxes!(during ingestion) to [quick, brown, fox] during analysis.
        //so you should use termQuery only to fields, type of that IS NOT "text"
    }


    //todo query with Aggregations

    public DeleteResponse deleteFromIndexById (String index, String id){
//        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://localhost:9200")));
        DeleteRequest deleteRequest = new DeleteRequest(index, id);

        try{
            DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
            System.out.println();
            return deleteResponse;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

}
