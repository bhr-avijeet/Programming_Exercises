import org.npntraining.hands_on.HiveConstants

import java.sql.{Connection, DriverManager, Statement}

object HiveDBConnection {
  def main(args: Array[String]): Unit = {

    val DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver"
    val CONNECTION_URL = "jdbc:hive2://192.168.234.154:10000/default"
    val USERNAME = "npntraining"
    val PASSWORD = "npntraining"
    // Step 01 : Load the Drivers
    Class.forName(DRIVER_NAME)
    println("Driver Loaded successfully")
    // Step 02 : Obtain the Connection object
    val con = DriverManager.getConnection(CONNECTION_URL, USERNAME, PASSWORD)
    println("Obtained Connection Object")
    // Step 03 : Get the statement object
    val stmt = con
      .createStatement
    // Step 04 : Execute the query
    stmt.execute("create database demodbdb")
    println("Database created successfully")
  }
}
