package com.elasticsearch.test.crudelastic.controller;

import com.elasticsearch.test.crudelastic.service.CSVEntityService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api")
public class CSVEntityController {

    private final CSVEntityService csvEntityService;

    @Autowired
    public CSVEntityController(CSVEntityService csvEntityService) {
        this.csvEntityService = csvEntityService;
    }

    @DeleteMapping(value = "/csventity/{entityId}")
    public ResponseEntity deleteCSVEntity(@PathVariable("entityId") String entityId) {
        csvEntityService.deleteById(entityId);
        return new ResponseEntity(HttpStatus.OK);
    }
}
