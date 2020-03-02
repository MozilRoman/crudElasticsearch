package com.elasticsearch.test.crudelastic.controller;

import com.elasticsearch.test.crudelastic.service.CSVEntityService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api")
public class CSVEntityController {

    private final CSVEntityService csvEntityService;

    @Autowired
    public CSVEntityController(CSVEntityService csvEntityService) {
        this.csvEntityService = csvEntityService;
    }

    @GetMapping("/indexes")
    public List<String> getIndexes() {
        return csvEntityService.getIndexes();
    }
}
