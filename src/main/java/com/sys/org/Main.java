package com.sys.org;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sys.org.connecton.ConnCreator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("Please provide file path.");
            System.exit(0);
        }
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Map<String, Object>> map = objectMapper.readValue(new File(args[0]), new TypeReference<Map<String, Map<String, Object>>>() {
        });

        try (ConnCreator connCreator = new ConnCreator()) {
            Main main = new Main();
            Connection fromDbConnection = connCreator.getConnection(map.get("from").get("host").toString(), map.get("from").get("port").toString(), map.get("from").get("databse").toString(), map.get("from").get("username").toString(), map.get("from").get("password").toString());
            Connection toDbConnection = connCreator.getConnection(map.get("to").get("host").toString(), map.get("to").get("port").toString(), map.get("to").get("databse").toString(), map.get("to").get("username").toString(), map.get("to").get("password").toString());
            String fromSchema = map.get("from").get("schema").toString();
            String toSchema = map.get("to").get("schema").toString();
            main.copyTimeSheet(fromDbConnection, toDbConnection, fromSchema, toSchema);
            main.copyTimeSheetInfo(fromDbConnection, toDbConnection, fromSchema, toSchema);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void copyTimeSheetInfo(Connection fromDbConnection, Connection toDbConnection, String fromSchema, String toSchema) throws SQLException {
        Statement statementFromDb = fromDbConnection.createStatement();
        ResultSet resultSetFromDb = statementFromDb.executeQuery("SELECT * FROM "+(isStringEmptyOrNull(fromSchema)?"":(fromSchema+"."))+"time_sheet_info");
        String insertQuery = "INSERT INTO "+(isStringEmptyOrNull(toSchema)?"":(toSchema+"."))+"time_sheet_info";
        String cols = "";
        String values = "";
        ResultSetMetaData metaData = resultSetFromDb.getMetaData();
        for (int i = 1; i < metaData.getColumnCount(); i++) {
            cols += metaData.getColumnName(i) + ", ";
            values += "?, ";
        }
        insertQuery += " (" + cols.substring(0, cols.lastIndexOf(",")) + ") " + "values(" + values.substring(0, values.lastIndexOf(",")) + ") ON CONFLICT DO NOTHING";
        PreparedStatement preparedStatement = toDbConnection.prepareStatement(insertQuery);
        while (resultSetFromDb.next()) {
            for (int i = 1; i < metaData.getColumnCount(); i++) {
                preparedStatement.setObject(i, resultSetFromDb.getObject(i));
            }
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
    }

    public void copyTimeSheet(Connection fromDbConnection, Connection toDbConnection, String fromSchema, String toSchema) throws SQLException {
        Statement statementFromDb = fromDbConnection.createStatement();
        ResultSet resultSetFromDb = statementFromDb.executeQuery("SELECT * FROM "+(isStringEmptyOrNull(fromSchema)?"":(fromSchema+"."))+"time_sheet");
        String insertQuery = "INSERT INTO "+(isStringEmptyOrNull(toSchema)?"":(toSchema+"."))+"time_sheet \n" +
                "            ( \n" +
                "                        time_sheet_id, \n" +
                "                        aggregate_id, \n" +
                "                        approved_by, \n" +
                "                        client_pay_date, \n" +
                "                        client_pay_date_internal, \n" +
                "                        comments, \n" +
                "                        created_by, \n" +
                "                        created_date, \n" +
                "                        created_time, \n" +
                "                        emp_id, \n" +
                "                        first_clock_in, \n" +
                "                        modified_by, \n" +
                "                        modified_date, \n" +
                "                        pay_date, \n" +
                "                        pending_with, \n" +
                "                        recent_clock_out, \n" +
                "                        request_time_zone, \n" +
                "                        schedule, \n" +
                "                        site_id, \n" +
                "                        status, \n" +
                "                        submission_status, \n" +
                "                        total_break, \n" +
                "                        total_overtime, \n" +
                "                        total_working, \n" +
                "                        update_action, \n" +
                "                        updated_number_time, \n" +
                "                        workflowid \n" +
                "            ) \n" +
                "            VALUES \n" +
                "            ( \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ?, \n" +
                "                        ? \n" +
                "            ) \n" +
                "on conflict do nothing";

        PreparedStatement preparedStatement = toDbConnection.prepareStatement(insertQuery);

        while (resultSetFromDb.next()) {
            Statement statementFromDbForTimeInfo = fromDbConnection.createStatement();
            ResultSet userActionTimestamp = statementFromDbForTimeInfo.executeQuery("SELECT user_action_timestamp,time_sheet_info_id FROM ws.time_sheet_info WHERE time_sheet_id=" + resultSetFromDb.getString("time_sheet_id") + " order by sort_order limit 1");
            if (userActionTimestamp.next()) {
                DateTime dateTime = new DateTime(userActionTimestamp.getString(1), DateTimeZone.UTC);
                preparedStatement.setObject(1, resultSetFromDb.getObject("time_sheet_id"));
                preparedStatement.setObject(2, "");
                preparedStatement.setObject(3, resultSetFromDb.getObject("approved_by"));
                preparedStatement.setObject(4, dateTime.toString());
                preparedStatement.setObject(5, dateTime.getMillis());
                preparedStatement.setObject(6, resultSetFromDb.getObject("comments"));
                preparedStatement.setObject(7, resultSetFromDb.getObject("created_by"));
                preparedStatement.setObject(8, resultSetFromDb.getObject("created_date"));
                preparedStatement.setObject(9, new Timestamp(dateTime.getMillis()));
                preparedStatement.setObject(10, resultSetFromDb.getObject("emp_id"));
                preparedStatement.setObject(11, resultSetFromDb.getObject("first_clock_in"));
                preparedStatement.setObject(12, resultSetFromDb.getObject("modified_by"));
                preparedStatement.setObject(13, resultSetFromDb.getObject("modified_date"));
                preparedStatement.setObject(14, resultSetFromDb.getObject("pay_date"));
                preparedStatement.setObject(15, resultSetFromDb.getObject("pending_with"));
                preparedStatement.setObject(16, resultSetFromDb.getObject("recent_clock_out"));
                preparedStatement.setObject(17, resultSetFromDb.getObject("request_time_zone"));
                preparedStatement.setObject(18, resultSetFromDb.getObject("schedule"));
                preparedStatement.setObject(19, resultSetFromDb.getObject("site_id"));
                preparedStatement.setObject(20, resultSetFromDb.getObject("submission_status"));
                preparedStatement.setObject(21, resultSetFromDb.getObject("total_break"));
                preparedStatement.setObject(22, resultSetFromDb.getObject("total_break"));
                preparedStatement.setObject(23, resultSetFromDb.getObject("total_overtime"));
                preparedStatement.setObject(24, resultSetFromDb.getObject("total_working"));
                preparedStatement.setObject(25, resultSetFromDb.getObject("update_action"));
                preparedStatement.setObject(26, 0);
                preparedStatement.setObject(27, resultSetFromDb.getObject("workflowid"));
                preparedStatement.addBatch();
            }
        }
        preparedStatement.executeBatch();
    }

    private boolean isStringEmptyOrNull(String str) {
        if (str != null && str.length() > 0) {
            return false;
        }
        return true;
    }
}
