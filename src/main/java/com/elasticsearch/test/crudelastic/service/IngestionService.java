package com.elasticsearch.test.crudelastic.service;

import com.elasticsearch.test.crudelastic.entity.CSVEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class IngestionService {

    private static String PATH = "C:\\Projects\\REFI_INFO\\ftp\\csv\\mr_clicks_export_indexattr_nov19.csv";// todo move to resources
    private static final String CSV_DELIMITER = ",";
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestionService.class);
    private static final Integer DEFAULT_ENTITY_ELEMENTS_LIST = 1;
    private static final Long DEFAULT_COUNT = 1L;

    private final CSVEntityService csvEntityService;
    private final ObjectMapper mapper;

    @Autowired
    public IngestionService(CSVEntityService csvEntityService, ObjectMapper mapper) {
        this.csvEntityService = csvEntityService;
        this.mapper = mapper;
    }

    public void loadCSV() {
        BufferedReader br = null;
        String line = "";
        boolean firstLine = true;

        try {
            br = new BufferedReader(new FileReader(PATH));

            int batch = 1000;
            int counter = 0;// Counter to check how many rows have been processed
            List<String> butchListToIngestion = new ArrayList<>();
            LocalDateTime startDate = LocalDateTime.now();

            while ((line = br.readLine()) != null) {
                if (firstLine) {
                    firstLine = false;
                    continue;
                }

                counter++;

                String[] columns = substringFirstAndLastChar(line).split("\",\""); //  \" mean "
                CSVEntity csvEntity = parseColumnsToCSVEntity(columns);

                if (counter % batch == 0) {
                    performBulkIngestion(butchListToIngestion);
                    LOGGER.info("Saved " + counter + " entity");
                } else {
                    butchListToIngestion.add(mapper.writeValueAsString(csvEntity));
                }

            }
            if (!butchListToIngestion.isEmpty()) {
                performBulkIngestion(butchListToIngestion);
                LOGGER.info("Saved " + counter + " entity");
            }

            LOGGER.info("Ingestion took = " + (Duration.between(startDate, LocalDateTime.now()).toMinutes()) + " minutes");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

//                    //serialization
//                    String json = mapper.writeValueAsString(csvEntity);
//                    //deserialization
//                    CSVEntity csvEntity1 = mapper.readValue(json ,CSVEntity.class);

    private static String substringFirstAndLastChar(String word) { //todo try save in Es not json, but entity(CSVEntity.class)
        return word.substring(1, word.length() - 1);
    }

    private void performBulkIngestion(List<String> butchListToIngestion) {
        csvEntityService.bulkIngestion(butchListToIngestion);
        butchListToIngestion.clear();
    }

    private static CSVEntity parseColumnsToCSVEntity(String[] columns) {
        CSVEntity csvEntity = new CSVEntity();
        csvEntity.setLastClickedMin(LocalDateTime.now());//todo make correct parsing
        if (columns[1].equals("null") || columns[1].equals("NULL")) {
            csvEntity.setVendorNewId("1");
        } else {
            csvEntity.setVendorNewId(columns[1]);
        }
        try {
            csvEntity.setCount(Long.valueOf(columns[2]));
        } catch (Exception e) {
            csvEntity.setCount(DEFAULT_COUNT);
        }
        csvEntity.setScrollId(columns[3]);
        csvEntity.setSubHeadLine(columns[4]);
        csvEntity.setSources(columns[5]);
        csvEntity.setRcsCodes(Arrays.asList(substringFirstAndLastChar(columns[6]).split(CSV_DELIMITER)));
        csvEntity.setVendorId(columns[7]);
        csvEntity.setRickcs(Arrays.asList(substringFirstAndLastChar(columns[8]).split(CSV_DELIMITER)));
        csvEntity.setPermIds(columns[9]);

        List<Integer> entityElementsList = new ArrayList<>();
        for (String element : substringFirstAndLastChar(columns[10]).split(CSV_DELIMITER)) {
            try {
                entityElementsList.add(Integer.valueOf(element.trim()));
            } catch (Exception e) {
                entityElementsList.add(DEFAULT_ENTITY_ELEMENTS_LIST);
            }
        }

        csvEntity.setEntitlements(entityElementsList);
        csvEntity.setHeadLang(columns[11]);
        return csvEntity;
    }

}
//https://medium.com/@pankaj_kumar_singh/bulk-insert-on-elasticsearch-7-1-using-java-high-level-rest-client-fdfc9e940a0d
