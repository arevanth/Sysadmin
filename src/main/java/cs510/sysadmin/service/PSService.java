package cs510.sysadmin.service;

import java.util.List;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

import org.springframework.web.bind.annotation.ResponseBody;

public class PSService {
	
	public String url;
	public Properties info;
	public ResponseBody body;
	
	public Properties getConnectionProperties(String db){
		Properties properties = new Properties();
		
		return properties;
	}
	
	public Connection getConnection(String db){
		
		Connection conn = null;
		try{
			Class.forName("com.mysql.jdbc.Driver");
		}
		
		catch(ClassNotFoundException e){
			System.out.println("JDBC Driver could not be found.");
		}
		
		url = "jdbc:mysql://localhost:3306/" + db + "?useSSL=false";
		//info = getConnectionProperties(db);
		
		System.out.println(url);
		
		try{
			conn = DriverManager.getConnection(url,"root","revanth$94");
		}
		catch(SQLException e){
			System.out.println("Could not establish a connection to the database");
		}
		
		return conn;
	}
	
	public ResponseBody start(String db, String command, String topic, String key, int index, int pagesize, int offset, String payload){
		
		Connection conn = null;
		try{
			Class.forName("com.mysql.jdbc.Driver");
		}
		
		catch(ClassNotFoundException e){
			System.out.println("JDBC Driver could not be found.");
		}
		
		url = "jdbc:mysql://localhost:3306/" + db + "?useSSL=false";
		//info = getConnectionProperties(db);
		
		System.out.println(url);
		try{
			conn = DriverManager.getConnection(url,"root","revanth$94");
		}
		catch(SQLException e){
			System.out.println("Could not establish a connection to the database");
		}
		
		switch (command) {
		case "CREATE":
			this.body = insertARecord(conn,db,topic,key,payload);
			break;

		default:
			break;
		}
		
		return this.body;
	}
	
	public ResponseBody insertARecord(Connection conn, String db, String topic, String key, String payload){
		
		Statement preparedStatement = null;
		int id=0;
		
		String insertSQLStatement = "INSERT INTO "+db+" (topic,pkey,payload) VALUES (" + "'" +topic + "','" + key + "','" + payload +"');";
		System.out.println(insertSQLStatement);
		
		try{
			
			preparedStatement = conn.createStatement();
			id = preparedStatement.executeUpdate(insertSQLStatement, Statement.RETURN_GENERATED_KEYS);
			
			ResultSet rs = preparedStatement.getGeneratedKeys();
			if(rs.next())
				id = rs.getInt(1);
		}
		
		catch(SQLException e){
			System.out.println("Could not add the record to the table.");
		}
		
		System.out.println(id);
		
		return this.body;
	}
	
	public ResponseBody deleteARecord(Connection conn, String db, String topic, int index){
		
		Statement preparedStatement = null;
		String deleteSQLStatement;
		
		deleteSQLStatement = "DELETE FROM " + db + " WHERE topic='" + topic + "' AND id=" + index;
		System.out.println(deleteSQLStatement);
		
		try{
			
			preparedStatement = conn.createStatement();
			preparedStatement.executeUpdate(deleteSQLStatement);
		}
		
		catch(SQLException e){
			System.out.println("Could not delete record from the table.");
		}
		
		return this.body;
	}
	
	public ResponseBody deleteARecord(Connection conn, String db, String topic, String key){
		Statement preparedStatement = null;
		String deleteSQLStatement;
		
		deleteSQLStatement = "DELETE FROM " + db + " WHERE topic='" + topic + "' AND pkey='" + key + "'";
		System.out.println(deleteSQLStatement);
		
		try{
			
			preparedStatement = conn.createStatement();
			preparedStatement.executeUpdate(deleteSQLStatement);
		}
		
		catch(SQLException e){
			System.out.println("Could not delete record from the table.");
		}
		
		return this.body;
	}
	
	public ResponseBody updateARecord(Connection conn, String db, String topic, String key, String payload){
		
		Statement preparedStatement = null;
		String updateSQLStatement;
		
		updateSQLStatement = "UPDATE " + db + " SET payload='" + payload +"' WHERE topic='" + topic + "' AND pkey='" + key + "';";
		System.out.println(updateSQLStatement);
		
		try{
			
			preparedStatement = conn.createStatement();
			preparedStatement.executeUpdate(updateSQLStatement);
		}
		
		catch(SQLException e){
			System.out.println("Could not execute the update operation");
		}
		
		return this.body;
	}
	
	public ResponseBody addTopic(Connection conn, String topic){
		Statement preparedStatement = null;
		String addTopicSQLStatement;
		
		addTopicSQLStatement = "INSERT INTO topic (topic) VALUES ('" + topic + "');";
		System.out.println(addTopicSQLStatement);
		
		try{
			preparedStatement = conn.createStatement();
			preparedStatement.executeUpdate(addTopicSQLStatement);
		}
		catch(SQLException e){
			System.out.println("Could not add topic to the database.");
		}
		
		return this.body;
	}
	
	public ResponseBody read(Connection conn, String table, String topic, String key){
		
		Statement prepStatement = null;
		String readSQLStatement;
		ResultSet rs;
		List<String> results = new ArrayList<String>();
		
		readSQLStatement = "SELECT payload from " + table + " WHERE pkey='" + key +"' AND topic='" + topic +"';";
		System.out.println(readSQLStatement);
		
		try{
			prepStatement = conn.createStatement();
			rs = prepStatement.executeQuery(readSQLStatement);
			
			while(rs.next()){
				String value = rs.getObject(1).toString(); 
				results.add(value);
			}
		}
		
		catch(SQLException e){
			System.out.println("Could not read.");
		}
		
		return this.body;
	}
	
	public ResponseBody read(Connection conn, String table, String topic, int index){
		
		Statement prepStatement = null;
		String readSQLStatement;
		ResultSet rs;
		List<String> results = new ArrayList<String>();
		
		readSQLStatement = "SELECT payload from " + table + " WHERE id='" + index +"' AND topic='" + topic +"';";
		System.out.println(readSQLStatement);
		
		try{
			prepStatement = conn.createStatement();
			rs = prepStatement.executeQuery(readSQLStatement);
			
			while(rs.next()){
				String value = rs.getObject(1).toString(); 
				results.add(value);
			}
		}
		
		catch(SQLException e){
			System.out.println("Could not read.");
		}
		
		return this.body;
	}
}
