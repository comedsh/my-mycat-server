package io.mycat.route.function;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import junit.framework.Assert;

public class MyPartitionByMurmurHashTest {
	
	HashFunction hashFunc = Hashing.murmur3_32( 0 );
	
	/**
	 * 通过 testCalculatedDuplicatePoints() 和 testIsSupportBigIntID() 两个测试用例，告诉我们，任意多个 ID 都可以映射到 2＾³² 环中的一个 point
	 * 因为多个 ID 可以对应一个 point;  point ¹<---->ⁿ ID
	 */
	
	/**
	 * 验证两个不同的 ID 通过 HashFunction 可以计算出相同的 point ( hash 环上的值 )
	 */
	@Test
	public void testCalculatedDuplicatePoints(){
		
		Assert.assertEquals("ID 1008983 和 1041150 得到相同的 point", hashFunc.hashUnencodedChars( Integer.toString(1008983) ).asInt(), hashFunc.hashUnencodedChars( Integer.toString(1041150) ).asInt() );
		
		Assert.assertEquals("ID 1008980 和 1041153 得到相同的 point", hashFunc.hashUnencodedChars( Integer.toString(1008980) ).asInt(), hashFunc.hashUnencodedChars( Integer.toString(1041153) ).asInt() );
		
		System.out.println(hashFunc.hashUnencodedChars( Integer.toString(10000) ).asInt());
		
	}
	
	/**
	 * 明白了，再长的数字它都可以转成 2＾³² 环中所对应的一位 point，因为它是将数字转换成了 Chars，通过 char 来计算得到对应的 point.
	 */
	@Test
	public void testIsSupportBigIntID(){
		
		long id = 111111111111111111L;
		
		hashFunc.hashUnencodedChars( new Long( id ).toString() );
		
	}
	
	/**
	 * 
	 * 创建测试 order: 
	 * 
	 * create table torder ( id int not null primary key auto_increment, customer_id int );
	 * 
	 * 模拟一种非常简单的关系，一个 customer 对应一个 order，目的是为了测试在跨库的情况下，两者之间的 joint 关系。
	 * 
	 */
	@Test
	public void testInitPartition(){

		
		Connection conn = null;
		
		Statement stmt = null;
        
        String url = "jdbc:mysql://127.0.0.1:8066/TESTDB?user=root&password=digdeep&useUnicode=true&characterEncoding=UTF8";
 
        try {
            
        	Class.forName("com.mysql.jdbc.Driver");// 动态加载mysql驱动
            
            conn = DriverManager.getConnection(url);
            
            stmt = conn.createStatement();		
            
            String sql = "drop table if exists torder";
            
            stmt.executeUpdate(sql);
            
            sql = "drop table if exists tcustomer";
            
            stmt.executeUpdate(sql);
            
            sql = "create table torder ( id int not null primary key auto_increment, customer_id int )";
            
            stmt.executeUpdate(sql);
            
            sql = "create table tcustomer ( id int not null primary key auto_increment, name varchar(30) )";
            
            stmt.executeUpdate(sql);
            
			int total = 10_0000; // 总共存放 10万条数据，但 ORDERID 从 1万开始计数。
			
			// ORDERID 正序递增，CUSTOMERID 反序递减
			
			for( int i = 1_0000; i <= total + 1_0000; i++){ //假设分片键从1万开始
				
				int customerid = ( total + 10000 ) - i; // 因为从 1W 开始计数，所以，+10000
				
				stmt.addBatch("insert into torder (ID, CUSTOMER_ID) values('"+i+"', '"+customerid+"')");
				stmt.addBatch("insert into tcustomer (ID, NAME) VALUES('"+customerid+"', 'NAME_"+customerid+"')");
				
				if( i % 10000 == 0 ){
					
					stmt.executeBatch();
					
					System.out.println("batch committed ~ ");
				
				}
				
				System.out.println("processed "+ i);
				
			}
			
			System.out.println("completed~~~~");
			
        }catch(Exception e){
        	
        	e.printStackTrace();
        	
        }finally{
        	
        
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
        	
        }
		
		
	}
	
	/**
	 * -jdbcDriver=com.mysql.jdbc.Driver -jdbcUrl=jdbc:mysql://127.0.0.1:8066/TESTDB?user=root&password=digdeep&useUnicode=true&characterEncoding=UTF8&rewriteBatchedStatements=true -rehashNodeDir=/Users/mac/temp -virtualBucketTimes=160 -hashType=MURMUR -shardingField=ID
	 */
	@Test
	public void testRehasher(){
		
	}
	
}
