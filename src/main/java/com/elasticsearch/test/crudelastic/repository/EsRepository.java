package com.elasticsearch.test.crudelastic.repository;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

//@Repository
@Component
public class EsRepository {
    //https://stackoverflow.com/questions/46854919/noclassdeffounderror-error-creating-resthighlevelclient-bean

    private final RestHighLevelClient client;
    private static final Logger LOGGER = LoggerFactory.getLogger(EsRepository.class);

    @Autowired
    public EsRepository(RestHighLevelClient client) {
        this.client = client;
    }

    public GetResponse get(GetRequest getRequest) {
        try {
            return client.get(getRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            LOGGER.warn("Failed to query Elastic Search" + e);//todo test output
        }
        return null;
    }

    public SearchResponse search(SearchRequest searchRequest) {
        try {
            return client.search(searchRequest, RequestOptions.DEFAULT);//todo make QueryBuilder and take out duplication with RequestOptions.DEFAULT
        } catch (Exception e) {
            LOGGER.warn("Failed to query Elastic Search" + e);
        }
        return null;
    }

    public DeleteResponse delete(DeleteRequest deleteRequest) {
        try {
            return client.delete(deleteRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            LOGGER.warn("Failed to query Elastic Search" + e);
        }
        return null;
    }

    public GetIndexResponse indexes(GetIndexRequest getIndexRequest) {
        try {
            return client.indices().get(getIndexRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            LOGGER.warn("Failed to query Elastic Search" + e);
        }
        return null;
    }

    public AcknowledgedResponse deleteIndex(DeleteIndexRequest deleteIndexRequest) {
        try {
            return client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            LOGGER.warn("Failed to query Elastic Search" + e);
        }
        return null;
    }

    public BulkResponse bulk(BulkRequest bulkRequest) {
        try {
            return client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            LOGGER.warn("Failed to query Elastic Search" + e);
        }
        return null;
    }

}
