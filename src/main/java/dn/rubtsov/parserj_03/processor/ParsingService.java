package dn.rubtsov.parserj_03.processor;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dn.rubtsov.parserj_03.config.MappingConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

@Slf4j
@Service
public class ParsingService {
    private final MappingConfiguration mappingConfiguration;

    public ParsingService(MappingConfiguration mappingConfiguration) {
        this.mappingConfiguration = mappingConfiguration;
    }

    /** Метод парсинга входящего json-файла в запись для внесения в БД */
    public List<Map<String, Object>> parsingJsonToRecordForDB(String json) throws JsonProcessingException {

        JsonNode rootNode = new ObjectMapper().readTree(json);

        // Используем список для хранения всех записей
        List<Map<String, Object>> records = new ArrayList<>();

        // Получаем одиночные поля
        String accountingDate = rootNode.at(mappingConfiguration.getFieldMappings().get("accountingDate"))
                .asText(null);
        String messageId = rootNode.at(mappingConfiguration.getFieldMappings().get("messageId"))
                .asText(null);
        String productId = rootNode.at(mappingConfiguration.getFieldMappings().get("productId"))
                .asText(null);

        // Обрабатываем массив registers
        JsonNode registersNode = rootNode.at(mappingConfiguration.getFieldMappings().get("registers"));
        if (registersNode.isArray()) {
            for (JsonNode register : registersNode) {
                Map<String, Object> record = new LinkedHashMap<>();
                record.put("productId", productId);
                record.put("messageId", messageId);
                record.put("accountingDate", accountingDate);
                record.put("registerType", register.at(mappingConfiguration.getFieldMappings().get("registerType"))
                        .asText(null));
                record.put("restIn", register.at(mappingConfiguration.getFieldMappings().get("restIn"))
                        .asText(null));
                // Добавляем запись в список
                records.add(record);
            }
        }
        return records;
    }

    /** Метод проверки обязательных полей на null или отсутствие в файле и удаления не валидных записей из списка. */
    public List<Map<String, Object>> deletingRecordsWithInvalidRequiredFields(List<Map<String, Object>> records) {
        // Для избежания ConcurrentModificationException при удалении в коллекции во время итерации
        List<Map<String, Object>> validRecords = new ArrayList<>();
        // Формируем список обязательных полей для их проверки на null
        List<String> requiredFields = new ArrayList<>(mappingConfiguration.getRequiredFields());
        System.out.println("Список обязательных полей: " + requiredFields);
        for (Map <String, Object> record : records) {
            boolean isValid = true;
            for (String regField : requiredFields) {
                if (record.containsKey(regField) && record.get(regField) == null) {
                    isValid = false;
                    log.warn("Пропускаем запись: обязательное поле {} = null или отсутствует", regField);
                    break; // Выход из цикла
                }
            }
            if(isValid) {
                validRecords.add(record);
            }
        }
        return validRecords;
    }
    /** Метод читает шаблон JSON из файла */
    public JsonNode readTempleFromFile () throws IOException {

        File jsonFile = Paths.get("src", "main", "resources", "test2.json").toFile();

        // Преобразовываем JSON файл в объект JsonNode
        JsonNode jsonTemplate = null;
        try {
            jsonTemplate = new ObjectMapper().readTree(jsonFile);
        } catch (IOException e) {
            throw new FileNotFoundException("Файла шаблона Json по указанному пути нет");
        }
        return jsonTemplate;
    }

}