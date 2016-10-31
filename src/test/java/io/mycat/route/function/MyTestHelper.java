package io.mycat.route.function;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class MyTestHelper {

	public static Connection getDBConnection( String url ){
		
		Connection conn = null;
        
        // String url = "jdbc:mysql://127.0.0.1:8066/TESTDB?user=root&password=digdeep&useUnicode=true&characterEncoding=UTF8";
            
    	try {
    		
			Class.forName("com.mysql.jdbc.Driver");
			
			conn = DriverManager.getConnection(url);
			
		} catch (ClassNotFoundException e) {
			
			e.printStackTrace();
			
			throw new RuntimeException(e);
			
		} catch (SQLException e) {
			
			e.printStackTrace();
			
			throw new RuntimeException(e);
		}

        return conn;
		
	}
	
	public static  Statement getStatement( Connection conn ){
		
		try {
			
			return conn.createStatement();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
			throw new RuntimeException(e);
		}
		
	}
	
}
