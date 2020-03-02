package com.elasticsearch.test.crudelastic.mainMethod;

import com.elasticsearch.test.crudelastic.entity.CSVEntity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
        import java.io.FileNotFoundException;
        import java.io.FileReader;
        import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class CSVReader {

    private static ObjectMapper mapper = new ObjectMapper();
    private static String cvsSplitBy = ",";
    private static final Logger LOGGER = LoggerFactory.getLogger(CSVReader.class);

    public static void main(String[] args) {

        String csvFile = "C:\\Projects\\REFI_INFO\\ftp\\csv\\mr_clicks_export_indexattr_nov19.csv";
        BufferedReader br = null;
        String line = "";


        //next lines
        List<CSVEntity> csvEntities = new ArrayList<>();
        int i = 0;
        setup();
        EsClient esClient = new EsClient();

        try {
            br = new BufferedReader(new FileReader(csvFile));

            int batch = 1000;
            // Conter to check how many rows have been processed
            int counter = 0;
            List<String> butchListToIngestion = new ArrayList<>();
            LocalDateTime startDate = LocalDateTime.now();
            LOGGER.info("startDate= " + startDate );

                while ((line = br.readLine()) != null) {

                    counter++;

                    String[] columns = substringFirstAndLastChar(line).split("\",\""); //  \" mean "
                    CSVEntity csvEntity = collectColumnsToCSVEntity(columns);

                    if (counter % batch == 0) {

                        esClient.bulkIngestionInIndex(butchListToIngestion, "newcsventity");
                        LOGGER.info("Saved " + counter + " entity");
//                        butchListToIngestion.clear();
                        butchListToIngestion = new ArrayList<>();
                    } else { //I think i loose the last accumulated values, and i exit from while loop not saving last batch
                        butchListToIngestion.add(mapper.writeValueAsString(csvEntity));
                    }

//                    //serialization
//                    String json = mapper.writeValueAsString(csvEntity);
//                    //deserialization
//                    CSVEntity csvEntity1 = mapper.readValue(json ,CSVEntity.class);

            }
            LOGGER.info("Took time = "+ (Duration.between(startDate, LocalDateTime.now()).toMinutes()) );

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

        System.out.println("Finish");

//        EsClient esClient = new EsClient();
////        String json ="";
////        List<String> listStringsOfCSVEntities = new ArrayList<>();
////        try {
////            json = mapper.writeValueAsString(csvEntities.get(0));
////            for(CSVEntity csvEntity : csvEntities){
////                listStringsOfCSVEntities.add( mapper.writeValueAsString(csvEntity));
////            }
////        } catch (JsonProcessingException e) {
////            e.printStackTrace();
////        }

//        esClient.putDocumentInIndex(json, "csventity");
//        esClient.getIndexes();
//        esClient.getDocumentFromIndexById("csventity", "1");
//        esClient.bulkIngestionInIndex(listStringsOfCSVEntities, "csventity");
//        esClient.deleteFromIndexById("csventity", "3_fShXAB0BYIWdgXluHe");
//        esClient.getDocumentFromIndexById("csventity", "1");
//        esClient.searchDocumentsFromIndexInFieldByName("csventity", "subHeadLine", "GIGAPHOTON");
//        esClient.searchDocumentsFromIndexInFieldByName("csventity", "sources", "BSW");
//        esClient.searchDocumentsFromIndexInFieldByExactValue("csventity", "count", "8");
//        esClient.updateDocumentFromIndexById("csventity", "4_fShXAB0BYIWdgXluHf", "count", "8");

    }

    //for serialization and deserialization LocalDateTime
    private static void setup(){
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    private static String substringFirstAndLastChar(String word){ //todo try save in Es not json, but entity(CSVEntity.class)
        return word.substring(1, word.length()-1);
    }

    private static CSVEntity collectColumnsToCSVEntity(String[] columns){
        CSVEntity csvEntity = new CSVEntity();
        csvEntity.setLastClickedMin(LocalDateTime.now());
        csvEntity.setVendorNewId( columns[1]);
        try{
            csvEntity.setCount(Long.valueOf( columns[2]));
        }
        catch (Exception e){
        }
        csvEntity.setScrollId(columns[3]);
        csvEntity.setSubHeadLine(columns[4]);
        csvEntity.setSources(columns[5]);
        csvEntity.setRcsCodes(Arrays.asList(substringFirstAndLastChar(columns[6]).split(cvsSplitBy)));
        csvEntity.setVendorId(columns[7]);
        csvEntity.setRickcs(Arrays.asList(substringFirstAndLastChar(columns[8]).split(cvsSplitBy)));
        csvEntity.setPermIds(columns[9]);

        List<Integer> entityElementsList = new ArrayList<>();
        for(String element : substringFirstAndLastChar(columns[10]).split(cvsSplitBy)){
            try{
                entityElementsList.add(Integer.valueOf(element.trim()));
            }
            catch (Exception e){
            }

        }
        csvEntity.setEntitlements(entityElementsList);
        csvEntity.setHeadLang(columns[11]);
        return csvEntity;
    }

}
//parsing csv
//https://mkyong.com/java/how-to-read-and-parse-csv-file-in-java/