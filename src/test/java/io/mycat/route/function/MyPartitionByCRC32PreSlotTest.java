package io.mycat.route.function;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

public class MyPartitionByCRC32PreSlotTest {
	
	@Test
	public void testInitPartition(){
		
		Connection conn = null;
        
        String url = "jdbc:mysql://127.0.0.1:8066/TESTDB?user=root&password=digdeep&useUnicode=true&characterEncoding=UTF8";
 
        try {
            
        	Class.forName("com.mysql.jdbc.Driver");// 动态加载mysql驱动
            
            conn = DriverManager.getConnection(url);
            
            Statement stmt = conn.createStatement();		
            
            String sql = "drop table if exists uorder";
            
            stmt.executeUpdate(sql);
            
            sql = "create table uorder ( id int not null primary key auto_increment, customer_id int )";
            
            stmt.executeUpdate(sql);
            
			int total = 10_0000; // 数据量 10万
			
			// ORDERID 正序，CUSTOMERID 反序
			
			for( int i = 1_0000, j=1_0000; i <= total + 1_0000; i++){ //假设分片键从1万开始
				
				stmt.addBatch("insert into uorder (ID, CUSTOMER_ID) values('"+i+"', '"+(total-i)+"')");
				
				if( i % 10000 == 0 ){
					
					stmt.executeBatch();
				
				}
				
			}
            
            
        }catch(Exception e){
        	
        	e.printStackTrace();
        	
        }finally{
        	
        	try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	
        }
	}
}
