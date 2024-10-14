package dn.rubtsov.parserj_03;

import dn.rubtsov.parserj_03.processor.DBUtils;
import dn.rubtsov.parserj_03.processor.JsonProducer;
import dn.rubtsov.parserj_03.processor.ParserJson;

import lombok.extern.slf4j.Slf4j;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@EnableConfigurationProperties
@SpringBootApplication
@EnableScheduling
@Slf4j
public class ParserJ_03Application implements CommandLineRunner {
    @Autowired
    ParserJson parserJson;
    @Autowired
    JsonProducer jsonProducer;
    @Autowired
    DBUtils dbUtils;

    public static void main(String[] args) {
        SpringApplication.run(ParserJ_03Application.class, args);
    }
    @Override
    public void run(String... args) {
        dbUtils.dropTableIfExists("message_db");
        dbUtils.createTableIfNotExists("message_db");

        try (InputStream inputStream = getClass().getResourceAsStream("/Test.json")) {
            // Проверяем, был ли успешно открыт InputStream
            if (inputStream == null) {
                log.error("Не удалось найти файл Test.json");
                return;
            }
            String jsonContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            parserJson.processJson(jsonContent,"message_db");
        } catch (IOException e) {
            log.error("Ошибка при чтении json-файла: {}", e.getMessage(), e);
        } catch (Exception e) {
            log.error("Ошибка при обработке json-файла: {}", e.getMessage(), e);
        }
    }
}