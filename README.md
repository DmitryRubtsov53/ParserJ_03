### Spring Boot-Kafka-приложение.

Адаптер, который парсит json из файла Test.json (src\main\resources\...) согласно маппингу (описанному в application.yaml) в таблицу БД PostgreSQL с колонками: 
- uid - уникальный ИД, 
- insert_date – дата/Время вставки строки в бд, 
- productid, 
- messageid, 
- accountingdate, 
- registertype, 
- restin, 
- dispatchstatus – по умолчанию = 0.

#### Если одно из обязательных полей null, то запись в БД не происходит. 
Обязательными полями считаются: 
- productid,
- registertype,
- restin.

#### Адаптер лезет в БД,забирает одну строку c dispatchStatus = 0
(забирает данные полей ( productid messageid accountingdate registertype restin ), проставляет в той строке, данные которой забрал dispatchStatus = 1, из данных, которые забрал формируем json по образцу test2.json (src\main\resources\...) и отправляем json в кафку.

#### Запуск маршрута происходит периодически - каждые 10 секунд. 
Т.о. при наличии в БД 3х строк с dispatchStatus = 0 должно пройти 3 цикла: забрал данные - собрал json - отправил в кафку После этого продолжаем проверять наличие интересующих нас строк каждые 10 секунд. Если данных нет, ждём - ждём бесконечно (как это делает реальный адаптер).

### Для запуска и проверки работы приложения (в IDE) нужно:
- Запустить docker, запустить docker-compouse (Zookiper, Kafka, БД PostgreSQL);
- Запустить ParserJ_04Application.
