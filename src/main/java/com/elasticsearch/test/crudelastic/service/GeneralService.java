package com.elasticsearch.test.crudelastic.service;

import com.elasticsearch.test.crudelastic.repository.GeneralRepository;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class GeneralService {

    private final GeneralRepository generalRepository;

    @Autowired
    public GeneralService(GeneralRepository generalRepository) {
        this.generalRepository = generalRepository;
    }

    public List<String> getIndexes() {
        return generalRepository.getIndexes();
    }

    public AcknowledgedResponse deleteIndex(String index){
        return generalRepository.deleteIndex(index);
    }

}
