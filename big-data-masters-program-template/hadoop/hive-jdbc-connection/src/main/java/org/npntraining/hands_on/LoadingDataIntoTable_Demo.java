package org.npntraining.hands_on;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class LoadingDataIntoTable_Demo {

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
            String query = "load data local inpath '${env:DATA_SET}/users1.dat' into table testdb.users";
            stmt.execute(query);
            System.out.println("Data loaded successfully");

        } catch (ClassNotFoundException e) {
            System.out.println("Driver not found");
        } catch (SQLException e) {
            System.out.println("Problem Connecting");
            e.printStackTrace();
        }
    }
}
