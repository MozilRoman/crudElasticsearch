package com.elasticsearch.test.crudelastic.controller;

import com.elasticsearch.test.crudelastic.service.CSVEntityService;
import com.elasticsearch.test.crudelastic.service.GeneralService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api")
public class GeneralController {

    private final GeneralService generalService;

    @Autowired
    public GeneralController(GeneralService generalService) {
        this.generalService = generalService;
    }

    @GetMapping("/general/indexes")
    public List<String> getIndexes() {
        return generalService.getIndexes();
    }

    @DeleteMapping(value = "/general")
    public ResponseEntity deleteIndex(@RequestParam("index") String index) {
        generalService.deleteIndex(index);
        return new ResponseEntity(HttpStatus.OK);
    }
}
