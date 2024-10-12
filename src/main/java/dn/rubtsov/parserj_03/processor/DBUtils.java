package dn.rubtsov.parserj_03.processor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;
@Service
public class DBUtils {

    private final String URL;
    private final String USER;
    private final String PASSWORD;

    public DBUtils(@Value("${spring.datasource.url}") String URL,
                   @Value("${spring.datasource.username}")String USER,
                   @Value("${spring.datasource.password}")String PASSWORD) {
        this.URL = URL;
        this.USER = USER;
        this.PASSWORD = PASSWORD;
    }

    /** Метод динамического создания таблицы для объекта.
     */
    public void createTableIfNotExists(String tableName) {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                "uid UUID PRIMARY KEY DEFAULT gen_random_uuid(), " +
                "insert_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(), " +
                "accountingDate VARCHAR(255), " +
                "messageId VARCHAR(255), " +
                "productid VARCHAR(255), " +
                "dispatchStatus INTEGER DEFAULT 0," +
                //я
                "registerType VARCHAR(255)," +
                "restIn VARCHAR(255))";

        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             Statement statement = connection.createStatement()) {
            statement.execute(createTableSQL);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /** Метод вставки данных объектов в таблицу.
     */
    public void insertRecords(Map<String, Object> data, String tableName) {
        if (data == null || data.isEmpty()) {
            return;  // Нет данных для вставки
        }

        // Создаем динамический SQL-запрос для вставки данных
        String insertDataSQL = createInsertSQL(tableName, new ArrayList<>(data.keySet()));
        System.out.println(insertDataSQL);

        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement preparedStatement = connection.prepareStatement(insertDataSQL)) {

            int i = 0;
            // Устанавливаем значения в запрос
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                Object value = entry.getValue();
                // Проверяем тип значения и устанавливаем в PreparedStatement
                if (value instanceof String) {
                    preparedStatement.setString(i + 1, (String) value);
                } else if (value instanceof Integer) {
                    preparedStatement.setInt(i + 1, (Integer) value);
                } else if (value instanceof Double) {
                    preparedStatement.setDouble(i + 1, (Double) value);
                } else if (value instanceof Boolean) {
                    preparedStatement.setBoolean(i + 1, (Boolean) value);
                } else if (value == null) {
                    preparedStatement.setNull(i + 1, java.sql.Types.NULL);
                } else {
                    throw new IllegalArgumentException("Unsupported data type: " + value.getClass().getName());
                }
                i++;
            }

            // Выполняем запрос на вставку
            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /** Метод для создания динамического SQL-запроса для вставки.
     */
    private static String createInsertSQL(String tableName, List<String> fieldNames) {
        String columns = String.join(", ", fieldNames);
        String valuesPlaceholder = String.join(", ", Collections.nCopies(fieldNames.size(), "?"));
        return "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + valuesPlaceholder + ")";
    }

    /** Универсальный метод для удаления таблицы.
     */
    public void dropTableIfExists(String tableName) {
        String dropSQL = "DROP TABLE IF EXISTS " + tableName + " CASCADE";
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             Statement statement = connection.createStatement()) {
            statement.execute(dropSQL);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /** Метод считывания 1-й записи БД с dispatchStatus = 0 и замены его на 1.
     * @return карта с парами ключ-значение полей считанной записи.
     */
    public Map<String, Object> getAndUpdateFirstRecordWithDispatchStatus() {
        // SQL-запрос для выборки данных из обеих таблиц с объединением
        String selectSQL = "SELECT uid, productid, messageid, accountingdate, registertype, restin FROM message_db WHERE dispatchStatus = 0 LIMIT 1";
        // SQL-запрос для обновления статуса записи в таблице
        String updateSQL = "UPDATE message_db SET dispatchStatus = 1 WHERE uid = ?";
        // Карта для хранения значений
        Map<String, Object> resultMap = new LinkedHashMap<>();

        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement selectStatement = connection.prepareStatement(selectSQL);
             PreparedStatement updateStatement = connection.prepareStatement(updateSQL);
             ResultSet resultSet = selectStatement.executeQuery()) {

            // Выборка первой записи
            if (resultSet.next()) {
                // Заполняем карту полями из результата выборки
                resultMap.put("uid", resultSet.getString("uid"));
                resultMap.put("productid", resultSet.getString("productid"));
                resultMap.put("messageid", resultSet.getString("messageid"));
                resultMap.put("accountingdate", resultSet.getString("accountingdate"));
                resultMap.put("registerType", resultSet.getString("registertype"));
                resultMap.put("restIn", resultSet.getInt("restin"));
                // Обновляем значение dispatchStatus в таблице по uid
                updateStatement.setObject(1, UUID.fromString((String) resultMap.get("uid")));
                updateStatement.executeUpdate();
                // Удаляем, так как uid нам нужен только для изменения dispatchStatus
                resultMap.remove("uid");

            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return resultMap;
    }
}
