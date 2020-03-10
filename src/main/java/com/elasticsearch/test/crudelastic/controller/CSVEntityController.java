package com.elasticsearch.test.crudelastic.controller;

import com.elasticsearch.test.crudelastic.service.CSVEntityService;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/api")
public class CSVEntityController {

    private final CSVEntityService csvEntityService;

    @Autowired
    public CSVEntityController(CSVEntityService csvEntityService) {
        this.csvEntityService = csvEntityService;
    }

    @GetMapping("/csventities")
    public SearchResponse getCSVEntities(@RequestParam("limit") Integer limit) {
        return csvEntityService.getCSVEntities(limit);
    }

    @GetMapping("/csventity/searchmatchquery")
    public SearchResponse getCSVEntitiesByFieldWithMatchQuery(@RequestParam("field") String field,
                                                              @RequestParam("value") String value) {
        return csvEntityService.getCSVEntitiesByFieldWithMatchQuery(field, value);
    }

    @GetMapping("/csventity/searchtermquery")
    public SearchResponse getCSVEntitiesByFieldWithTermQuery(@RequestParam("field") String field,
                                                             @RequestParam("value") String value) {
        return csvEntityService.getCSVEntitiesByFieldWithTermQuery(field, value);
    }

    @GetMapping("/csventity/popular")//execute POST from resources.forAggregation.txt to enable fieldData
    public SearchResponse getMostPopularCSVEntities(@RequestParam("startDate") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime startDate,
                                                    @RequestParam("endDate") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime endDate,
                                                    @RequestParam("minCounter") Long minCounter) {
        return csvEntityService.getMostPopularCSVEntities(startDate, endDate, minCounter);
    }

    @GetMapping("/csventity/updatebyquery")
    public BulkByScrollResponse updateCSVEntitiesByQuery(@RequestParam("field") String field,
                                                         @RequestParam("oldValue") String oldValue,
                                                         @RequestParam("newValue") String newValue) {
        return csvEntityService.updateCSVEntitiesByQuery(field, oldValue, newValue);
    }

    @DeleteMapping(value = "/csventity/{entityId}")
    public ResponseEntity deleteCSVEntity(@PathVariable("entityId") String entityId) {
        csvEntityService.deleteCSVEntityById(entityId);
        return new ResponseEntity(HttpStatus.OK);
    }
}
