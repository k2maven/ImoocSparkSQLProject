package com.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLUtils {
	 /**
	    * 获取数据库连接
	 * @throws SQLException 
	    */
	
	public static Connection getConnection() throws SQLException {
		
		return DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_project?user=root&password=root");
	}
	
	public static void release(Connection connection, PreparedStatement pstmt) throws SQLException {
		try {
			if (pstmt != null) {
				pstmt.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}
}
