package dn.rubtsov.parserj_03;

import dn.rubtsov.parserj_03.processor.JsonProducer;
import dn.rubtsov.parserj_03.processor.ParserJson;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class ParserJsonTest {
    @Autowired
    ParserJson parserJson;

    @Autowired
    JsonProducer jsonProducer;


//    @BeforeAll
//    static void setUp() {
//        DBUtils.createTableIfNotExists("message_db");
//    }

//    @AfterAll
//    static void tearDown(@Autowired JsonProducer jsonProducer) {
//        DBUtils.dropTableIfExists("message_db");
//        if (jsonProducer != null) {
//            jsonProducer.close();
//        }
//    }

    @Test
    void processJson() {
    }

    @Test
    void messageDBToJson() {
    }
}