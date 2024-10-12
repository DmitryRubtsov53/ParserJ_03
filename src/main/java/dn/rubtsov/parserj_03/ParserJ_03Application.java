package dn.rubtsov.parserj_03;

import dn.rubtsov.parserj_03.processor.DBUtils;
import dn.rubtsov.parserj_03.processor.JsonProducer;
import dn.rubtsov.parserj_03.processor.ParserJson;
import jakarta.annotation.PreDestroy;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@EnableConfigurationProperties
@SpringBootApplication
@EnableScheduling
public class ParserJ_03Application implements CommandLineRunner {

    @Autowired
    ParserJson parserJson;
    @Autowired
    JsonProducer jsonProducer;
    @Autowired
    DBUtils dbUtils;

    @PreDestroy
    public void destroy(){
        dbUtils.dropTableIfExists("message_db");

    }

    public static void main(String[] args) {
        SpringApplication.run(ParserJ_03Application.class, args);
    }
    @Override
    public void run(String... args) {
        dbUtils.createTableIfNotExists("message_db");
        try (InputStream inputStream = getClass().getResourceAsStream("/Test.json")) {
            String jsonContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            parserJson.processJson(jsonContent,"message_db");
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}