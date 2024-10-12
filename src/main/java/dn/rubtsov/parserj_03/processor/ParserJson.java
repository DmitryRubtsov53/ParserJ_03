package dn.rubtsov.parserj_03.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dn.rubtsov.parserj_03.config.MappingConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

@Component
public class ParserJson {
    private final MappingConfiguration mappingConfiguration;
    @Autowired
    JsonProducer jsonProducer;
    @Autowired
    DBUtils dbUtils;
    @Autowired
    public ParserJson(MappingConfiguration mappingConfiguration) {
        this.mappingConfiguration = mappingConfiguration;
    }

    public void processJson(String json, String tableName) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(json);

        // Используем список для хранения всех записей
        List<Map<String, Object>> records = new ArrayList<>();

        // Получаем одиночные поля
        String accountingDate = rootNode.at(mappingConfiguration.getFieldMappings().get("accountingDate")).asText(null);
        String messageId = rootNode.at(mappingConfiguration.getFieldMappings().get("messageId")).asText(null);
        String productId = rootNode.at(mappingConfiguration.getFieldMappings().get("productId")).asText(null);

        // Проверяем обязательные поля
        if ( productId == null) {
            System.out.println("Пропускаем запись: обязательные поля не заполнены.");
            return; 
        }

        // Обрабатываем массив registers
        JsonNode registersNode = rootNode.at(mappingConfiguration.getFieldMappings().get("registers"));
        if (registersNode.isArray()) {
            for (JsonNode register : registersNode) {
                // Создаем запись для каждого элемента массива
                Map<String, Object> record = new LinkedHashMap<>();
                record.put("productId", productId);
                record.put("messageId", messageId);
                record.put("accountingDate", accountingDate);
                record.put("registerType", register.at(mappingConfiguration.getFieldMappings().get("registerType")).asText(null));
                record.put("restIn", register.at(mappingConfiguration.getFieldMappings().get("restIn")).asText(null));

                // Проверяем наличие обязательных полей перед добавлением записи
                if (record.get("registerType") == null || record.get("restIn") == null) {
                    System.out.println("Пропускаем запись: обязательные поля в registers не заполнены.");
                    continue; 
                }
                // Добавляем запись в список
                records.add(record);
            }
        }
        // Выводим список записей
        System.out.println(records);

        // Обработка для вставки в базу данных
        for (Map<String, Object> record : records) {
            dbUtils.insertRecords(record, tableName);
        }
    }

    /** Метод замены значений полей test2.json на значения одноименных
     * полей записи из таблицы message_db БД и отправки в kafka.
     */
    @Scheduled(cron = "1/10 * * * * ?")
    public void MessageDBToJson() {
        try {
            // Получаем требуемые данные из базы
            Map<String,Object> messageDB = dbUtils.getAndUpdateFirstRecordWithDispatchStatus();
            if (messageDB.isEmpty()) {
                System.out.println("Нет данных для обработки.");
                return;
            }

            // Читаем шаблон JSON из файла
            ObjectMapper objectMapper = new ObjectMapper();
            File jsonFile = Paths.get("src", "main", "resources", "test2.json").toFile();

            // Парсим JSON файл в объект JsonNode
            JsonNode jsonTemplate = null;
            try {
                jsonTemplate = objectMapper.readTree(jsonFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            // Рекурсивно мапим объект messageDB на JSON-шаблон
            mapFieldsToJson(messageDB, jsonTemplate);

            // Преобразуем итоговый объект JsonNode обратно в строку
            System.out.println(objectMapper.writeValueAsString(jsonTemplate));
            jsonProducer.sendMessage(objectMapper.writeValueAsString(jsonTemplate));

        } catch (IOException | IllegalAccessException e) {
            System.err.println("Ошибка при обработке JSON или данных из базы.");
            e.printStackTrace();
        }
    }

    // Метод для рекурсивного маппинга полей объекта на JSON
    private static void mapFieldsToJson(Map<String, Object> messageDB, JsonNode jsonNode) throws IllegalAccessException {
        for (Map.Entry<String,Object> field : messageDB.entrySet()) {
            Object value = field.getValue();  

            if (value != null) {
                replaceValueInJson(jsonNode, field.getKey(), value);
            }
        }
    }

    // Метод для замены значения в JSON с учетом массивов
    private static void replaceValueInJson(JsonNode jsonNode, String fieldName, Object value) {
        // Приводим искомое имя поля к нижнему регистру
        String fieldNameLower = fieldName.toLowerCase();

        if (jsonNode.isObject()) {
            // Проверяем наличие ключа без учета регистра
            for (Iterator<String> it = ((ObjectNode) jsonNode).fieldNames(); it.hasNext(); ) {
                String key = it.next();
                if (key.equalsIgnoreCase(fieldName)) {
                    // Заменяем значение, если ключ найден
                    ((ObjectNode) jsonNode).put(fieldName, value.toString());
                    return; 
                }
            }
        }

        // Проходим по дочерним узлам и проверяем массивы
        for (JsonNode childNode : jsonNode) {
            if (childNode.isObject()) {
                replaceValueInJson(childNode, fieldName, value);
            } else if (childNode.isArray()) {
                // Если узел является массивом, проходим по каждому элементу массива
                for (JsonNode arrayItem : childNode) {
                    if (arrayItem.isObject()) {
                        replaceValueInJson(arrayItem, fieldName, value);
                    }
                }
            }
        }
    }

}
