package dn.rubtsov.parserj_03.processor;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dn.rubtsov.parserj_03.config.MappingConfiguration;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

@Component
@Slf4j
public class ParserJson {

    private final JsonProducer jsonProducer;
    private final DBUtils dbService;
    private final ParsingService parsingService;
    @Autowired
    public ParserJson(JsonProducer jsonProducer, DBUtils dbService,
                      ParsingService parsingService) {
        this.jsonProducer = jsonProducer;
        this.dbService = dbService;
        this.parsingService = parsingService;
    }

    public void processJson(String json, String tableName) throws Exception {
        // Список для хранения всех записей
        List<Map<String, Object>> records = new ArrayList<>(parsingService.parsingJsonToRecordForDB(json));

        // Для отладки
        System.out.println("Список всех записей для внесения в БД: \n" + records);

        // Проверка обязательных полей и удаление не валидных записей из списка
        records = parsingService.deletingRecordsWithInvalidRequiredFields(records);

        // Обработка для вставки в БД
        for (Map<String, Object> rec : records) {
            System.out.println("record для вставки: " + rec);
            dbService.insertRecords(rec, tableName);
        }
    }

    /** Метод замены значений полей test2.json на значения одноименных
     * полей записи из таблицы message_db БД и отправки в kafka.
     */
    @Scheduled(cron = "1/10 * * * * ?")
    public void MessageDBToJson() {
        try {
            // Получаем требуемые данные из базы
            Map<String,Object> messageDB = dbService.getAndUpdateFirstRecordWithDispatchStatus();
            if (messageDB.isEmpty()) {
                log.info("Нет данных для обработки.");
                return;
            }
            // Читаем шаблон JSON из файла
            JsonNode jsonTemplate = parsingService.readTempleFromFile();

            // Рекурсивно мап пим объект messageDB на JSON-шаблон
            mapFieldsToJson(messageDB, jsonTemplate);

            // Преобразуем итоговый объект JsonNode обратно в строку
            String json = new ObjectMapper().writeValueAsString(jsonTemplate);

            log.info("Сообщение: {}", json);
            jsonProducer.sendMessage(json);

        } catch (IOException | IllegalAccessException e) {
            log.error("Ошибка при обработке JSON или данных из базы.",e);
        }
    }

    // Метод для рекурсивного маппинг полей объекта на JSON
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