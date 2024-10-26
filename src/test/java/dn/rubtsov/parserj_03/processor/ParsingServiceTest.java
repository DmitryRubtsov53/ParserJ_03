package dn.rubtsov.parserj_03.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dn.rubtsov.parserj_03.config.MappingConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
@SpringBootTest
class ParsingServiceTest {

    @Mock
    private MappingConfiguration mappingConfiguration;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private ParsingService parsingService;


    @BeforeEach
    void setUp() {
        mappingConfiguration = mock(MappingConfiguration.class);
        when(mappingConfiguration.getFieldMappings()).thenReturn(createFieldMappings());
        when(mappingConfiguration.getRequiredFields()).thenReturn(asList("productId", "restIn"));
        parsingService = new ParsingService(mappingConfiguration);
    }

    private Map<String, String> createFieldMappings() {
        Map<String, String> fieldMappings = new HashMap<>();
        fieldMappings.put("accountingDate", "/accountingDate");
        fieldMappings.put("messageId", "/messageId");
        fieldMappings.put("productId", "/productId");
        fieldMappings.put("registers", "/registers");
        fieldMappings.put("registerType", "/registerType");
        fieldMappings.put("restIn", "/restIn");
        return fieldMappings;
    }

    @Test
    void testParsingJsonToRecordForDB_Positive() throws JsonProcessingException {
        String json = "{ \"accountingDate\":\"2024-10-24\", \"messageId\":\"123\", \"productId\":\"456\", " +
                "\"registers\": [{\"registerType\":\"type1\", \"restIn\":\"100\"}, " +
                "{\"registerType\":\"type2\", \"restIn\":\"50\"}] }";

        List<Map<String, Object>> records = parsingService.parsingJsonToRecordForDB(json);

        assertEquals(2, records.size());
        assertEquals("456", records.get(0).get("productId"));
        assertEquals("123", records.get(0).get("messageId"));
        assertEquals("2024-10-24", records.get(0).get("accountingDate"));
        assertEquals("type1", records.get(0).get("registerType"));
        assertEquals("100", records.get(0).get("restIn"));
    }
    @Test
    void testParsingJsonToRecordForDB_Negative() {
        System.out.println(
                "Негативного сценария не существует, т.к. метод формирует предварительный список всех \n" +
                        "входящих записей без фильтрации по валидности, которая производится потом в методе \n"
                        + "DeletingRecordsWithInvalidRequiredFields.");
    }

    @Test
    void testDeletingRecordsWithInvalidRequiredFields_Negative() {
        List<Map<String, Object>> records = new ArrayList<>();
        // Valid field
        Map<String, Object> validRecord = new HashMap<>();
        validRecord.put("productId", "456");
        validRecord.put("messageId", null);
        validRecord.put("restIn", 50);
        records.add(validRecord);
        // Invalid field
        Map<String, Object> invalidRecord = new HashMap<>();
        invalidRecord.put("productId", null);
        invalidRecord.put("messageId", "123");
        invalidRecord.put("restIn",100);
        records.add(invalidRecord);
        System.out.println("records: " + records);

        List<Map<String, Object>> filteredRecords = parsingService.deletingRecordsWithInvalidRequiredFields(records);
        System.out.println("filteredRecords: " + filteredRecords);

        assertEquals(1,filteredRecords.size());
        // Проверяем, что осталась только валидная запись
        assertEquals("456", filteredRecords.get(0).get("productId"));
    }

    @Test
    void testDeletingRecordsWithInvalidRequiredFields_Positive() {
        List<Map<String, Object>> records = new ArrayList<>();
        Map<String, Object> validRecord = new HashMap<>();
        validRecord.put("productId", "456");
        validRecord.put("restIn", "100");
        records.add(validRecord);

        List<Map<String, Object>> filteredRecords = parsingService.deletingRecordsWithInvalidRequiredFields(records);

        assertEquals(1, filteredRecords.size());
        assertEquals("456", filteredRecords.get(0).get("productId"));
    }

    @Test
    void testReadTempleFromFile_Positive() throws IOException {
        JsonNode jsonTemple = parsingService.readTempleFromFile();
        assertNotNull(jsonTemple);
    }

}