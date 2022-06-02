package org.npntraining.hands_on;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveDBConnection {

    public static void main(String[] args) {
        try {
            // Step 01 : Load the Drivers
            Class.forName(HiveConstants.DRIVER_NAME);
            System.out.println("Driver Loaded successfully");
            // Step 02 : Obtain the Connection object
            Connection con = DriverManager.getConnection(
                    HiveConstants.CONNECTION_URL, HiveConstants.USERNAME,
                    HiveConstants.PASSWORD);
            System.out.println("Obtained Connection Object");
			// Step 03 : Get the statement object
            Statement stmt = con.createStatement();
			// Step 04 : Execute the query
            stmt.execute("create database demo_db");
            System.out.println("Database created successfully");

        } catch (ClassNotFoundException e) {
            System.out.println("Driver not found");
        } catch (SQLException e) {
            System.out.println("Problem Connecting");
            e.printStackTrace();
        }

    }

}
