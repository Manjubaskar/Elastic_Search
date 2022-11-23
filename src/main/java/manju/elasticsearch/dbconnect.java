package manju.elasticsearch;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.databind.ObjectMapper;

import manju.elasticsearch.entity.Person;
import manju.elasticsearch.entity.Registration;


/**
 * This method used to connect the db and values taken from table
 * @author e1066
 *
 */
public class dbconnect {

	   //The config parameters for the  postgres connection

	   static final String DB_URL = "jdbc:postgresql://localhost:5432/mypull";
	   static final String USER = "madhu";
	   static final String PASS = "manjubaskar";
	   static final String QUERY = "SELECT id, first_name, last_name, age FROM Registration";

	   
	   //The config parameters for the elastic search connection
	    private static final String HOST = "localhost";
	    private static final int PORT_ONE = 9200;
	    private static final String SCHEME = "http";

	    private static RestHighLevelClient restHighLevelClient;
	    private static ObjectMapper objectMapper = new ObjectMapper();

	    private static final String INDEX = "registrationdata";
	    private static final String TYPE = "registration";

	    
	    /**
	     * This method used for elastic search host make connection
	     * @return
	     */
	    private static synchronized RestHighLevelClient makeConnection() {

	        if(restHighLevelClient == null) {
	            restHighLevelClient = new RestHighLevelClient(
	                    RestClient.builder(
	                            new HttpHost(HOST, PORT_ONE, SCHEME)));
			        }
			
			        return restHighLevelClient;
			    }

	    /**
	     * This method used for elastic search host close connection
	     * @return
	     */
	    private static synchronized void closeConnection() throws IOException {
	        restHighLevelClient.close();
	        restHighLevelClient = null;
	    }
	    
	    /**
	     * This method used for post the index and values
	     * @param registration
	     * @return
	     */
	    private static Registration insertPerson(Registration registration){
	        Map<String, Object> dataMap = new HashMap<String, Object>();
	        dataMap.put("id", registration.getId());
	        dataMap.put("first_name", registration.getFirst_name());
	        dataMap.put("last_name", registration.getLast_name());
	        dataMap.put("age", registration.getAge());
	        IndexRequest indexRequest = new IndexRequest(INDEX, TYPE)
	                .source(dataMap);
	        try {
	            IndexResponse response = restHighLevelClient.index(indexRequest);
	            System.out.println(response);

	        } catch(Exception ex) {
		    	ex.printStackTrace();
		    	}
	        return registration;
	    }

	    /**
	     * This method used to call the functions required
	     * @param args
	     * @throws IOException
	     */
	    public static void main(String[] args) throws IOException {
		   

		      // Open a connection
		      try(Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
		         Statement stmt = conn.createStatement();
		    	 ResultSet rs = stmt.executeQuery(QUERY);

		      ) {	
		    	  
		    	  // Table creation in db
		          String sql1 = "CREATE TABLE REGISTRATION " +
		                   "(id SERIAL, " +
		                   " first_name VARCHAR(255), " + 
		                   " last_name VARCHAR(255), " + 
		                   " age INTEGER)"; 

		         stmt.executeUpdate(sql1);
		         System.out.println("Created table in given database...");  
		         
		         // Insert table data 		         
		         
		         System.out.println("Inserting records into the table...");          
		         String sql = "INSERT INTO Registration VALUES (100, 'Zara', 'Ali', 18)";
		         stmt.executeUpdate(sql);
		         sql = "INSERT INTO Registration VALUES (101, 'Mahnaz', 'Fatma', 25)";
		         stmt.executeUpdate(sql);
		         sql = "INSERT INTO Registration VALUES (102, 'Zaid', 'Khan', 30)";
		         stmt.executeUpdate(sql);
		         sql = "INSERT INTO Registration VALUES(103, 'Sumit', 'Mittal', 28)";
		         stmt.executeUpdate(sql);
		         System.out.println("Inserted records into the table...");
		         
		    	  
		         // Get data from db using query
		    	  makeConnection();
		    	  
		    	  Registration registration = new Registration();
		    	  while(rs.next()){
		    		  
		              //Display values
		    		  registration.setId(rs.getInt("id"));
		    		  registration.setFirst_name(rs.getString("first_name"));
		    		  registration.setLast_name(rs.getString("last_name"));
		    		  registration.setAge(rs.getInt("age"));

		    		  registration = insertPerson(registration);

		    	  	} 
		    	  
		    	  closeConnection();
		    	  
		    	  }catch (SQLException e) {
		         e.printStackTrace();
		         System.out.println("azsCFG..");   	  

		      } 
		   }
		}