package org.npntraining.hands_on;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class CreatingTable_Demo {

    public static void main(String[] args) {
        try {
            Class.forName(HiveConstants.DRIVER_NAME);
            System.out.println("Driver Loaded successfully");
            Connection con = DriverManager.getConnection(
                    HiveConstants.CONNECTION_URL,
                    HiveConstants.USERNAME,
                    HiveConstants.PASSWORD);
            System.out.println("Obtained Connection Object");
            Statement stmt = con.createStatement();
            String query = "CREATE TABLE testdb.users(\n" +
                    "    user_id INT COMMENT 'user Id',\n" +
                    "    first_name STRING COMMENT 'user name',\n" +
                    "    last_name STRING,\n" +
                    "    gender STRING,\n" +
                    "    designation STRING,\n" +
                    "    city STRING,\n" +
                    "    country STRING,\n" +
                    "    dob DATE\n" +
                    ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' Stored as TextFile\n" +
                    "tblproperties (\"skip.header.line.count\"=\"1\",\"skip.footer.line.count\"=\"1\")";
            System.out.println(query);
            stmt.execute(query);
            System.out.println("Table created successfully");

        } catch (ClassNotFoundException e) {
            System.out.println("Driver not found");
        } catch (SQLException e) {
            System.out.println("Problem Connecting");
            e.printStackTrace();
        }
    }
}
