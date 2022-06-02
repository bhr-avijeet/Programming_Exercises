package org.npntraining.hands_on;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class SelectTable {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	// private static String driverName="javax.jdo.option.ConnectionDriverName";

	public static void main(String[] args) {
		try {
			Class.forName(driverName);
			System.out.println("Driver Loaded successfully");
			Connection con = DriverManager.getConnection(
					"jdbc:hive2://localhost:10000/success", "npntraining",
					"npntraining");
			System.out.println("Obtained Connection Object");
			Statement stmt = con.createStatement();
			String query = "select * from employee2";
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				System.out.println(rs.getInt(1));
				System.out.println(rs.getString(2));
//				System.out.println(rs.getObject("id").toString() + "\t"
//						+ rs.getObject("name") + "\t" + rs.getObject("dept")
//						+ "\t" + rs.getObject("salary"));
				//System.out.println(rs.getNString("id"));
				//System.out.println(rs.getInt(1));
			}
			System.out.println("****************");
			ResultSetMetaData rsmd=rs.getMetaData();
			int numOfColumn=rsmd.getColumnCount();
			for(int i=1;i<=numOfColumn;i++)
			{
				System.out.print(rsmd.getColumnName(i)+"\t");
			}
			System.out.println("Table created successfully");

		} catch (ClassNotFoundException e) {
			System.out.println("Driver not found");
		} catch (SQLException e) {
			System.out.println("Problem Connecting");
			e.printStackTrace();
		}
	}

}
