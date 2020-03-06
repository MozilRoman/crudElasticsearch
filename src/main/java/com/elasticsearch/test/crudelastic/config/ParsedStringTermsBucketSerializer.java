package com.elasticsearch.test.crudelastic.config;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;

import java.io.IOException;

public class ParsedStringTermsBucketSerializer extends StdSerializer<ParsedStringTerms.ParsedBucket> {

    public ParsedStringTermsBucketSerializer(Class<ParsedStringTerms.ParsedBucket> t) {
        super(t);
    }

    @Override
    public void serialize(ParsedStringTerms.ParsedBucket value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeStartObject();
        gen.writeObjectField("aggregations", value.getAggregations());
        gen.writeObjectField("key", value.getKey());
        gen.writeStringField("keyAsString", value.getKeyAsString());
        gen.writeNumberField("docCount", value.getDocCount());
        gen.writeEndObject();
    }
}
//Resolved [org.springframework.http.converter.HttpMessageNotWritableException: Could not write JSON: For input string: "null"; nested exception is com.fasterxml.jackson.databind.JsonMappingException: For input string: "null" (through reference chain: org.elasticsearch.action.search.SearchResponse["aggregations"]->org.elasticsearch.search.aggregations.Aggregations["asMap"]->java.util.Collections$UnmodifiableMap["sum_counts"]->org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms["buckets"]->java.util.ArrayList[0]->org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms$ParsedBucket["keyAsNumber"])]
//https://programmer.help/blogs/json-serialization-of-terms-aggregation-results-in-elasticsearch.html
//another one. but don`t works
//https://stackoverflow.com/questions/47792915/getting-jackson-parsing-error-while-serializing-aggregatedpage-in-spring-data-el
