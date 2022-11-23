package manju.elasticsearch;

import java.io.IOException;
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


public class elasticApiCalls {

	    //The config parameters for the elastic search connection
	    private static final String HOST = "localhost";
	    private static final int PORT_ONE = 9200;
	    private static final String SCHEME = "http";

	    private static RestHighLevelClient restHighLevelClient;
	    private static ObjectMapper objectMapper = new ObjectMapper();

	    private static final String INDEX = "persondata";
	    private static final String TYPE = "person";

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
	     * Insert a new person
	     * @param person
	     * @return
	     */
	    private static Person insertPerson(Person person){
	        person.setPersonId(UUID.randomUUID().toString());
	        Map<String, Object> dataMap = new HashMap<String, Object>();
	        dataMap.put("personId", person.getPersonId());
	        dataMap.put("name", person.getName());
	        IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, person.getPersonId())
	                .source(dataMap);
	        try {
	            IndexResponse response = restHighLevelClient.index(indexRequest);
	            System.out.println(response);

	        } catch(Exception ex) {
		    	ex.printStackTrace();
		    	}
	        return person;
	    }

	    /**
	     * Get a person by Id
	     * @param id
	     * @return
	     */
	    
	    private static Person getPersonById(String id){
	        GetRequest getPersonRequest = new GetRequest(INDEX, TYPE, id);
	        GetResponse getResponse = null;
	        try {
	            getResponse = restHighLevelClient.get(getPersonRequest);
	        } catch(Exception ex) {
		    	ex.printStackTrace();
		    	} 
	        return getResponse != null ?
	                objectMapper.convertValue(getResponse.getSourceAsMap(), Person.class) : null;
	    }

	    /**
	     * Update a person by Id
	     * @param id
	     * @param person
	     */
	    private static void updatePersonById(String id, Person person){
	    	  try {
	    		  Person data=getPersonById(id);
	    		  
	    		  person.setPersonId(id);
	              System.out.println(person);
	              if(data==null) {
	            	  System.out.println("person Id not found");}
	              else {
	    			  System.out.println(person.getPersonId());
	                  System.out.println(person.getName());

		    			person.setPersonId(id);
		    			UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, id).fetchSource(true);    // Fetch Object after its update
					        try {
					        	
					            String personJson = objectMapper.writeValueAsString(person);
					            updateRequest.doc(personJson, XContentType.JSON);
					            System.out.println(updateRequest);

					            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest);
					            System.out.println("person data updated");
					            }
					        	catch(Exception ex) {
						    	ex.printStackTrace();}
	                          	}
	    	  		}
		    	 catch(Exception ex) {
				 ex.printStackTrace();}
	    }

	    /**
	     * Delete a Person by Id
	     * @param id
	     */
	    private static void deletePersonById(String id) {
	        DeleteRequest deleteRequest = new DeleteRequest(INDEX, TYPE, id);
	        try {
	        	Person person=getPersonById(id);
	        	
	        	if(person==null) {
	        		System.out.println("person Id not found");
	        		} else {
	           
	            DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest);
	            System.out.println("Person Deleted");
	            System.out.println(deleteResponse);
	        	}
	        } catch(Exception ex) {
		    	ex.printStackTrace();
		    	}
	    }

	    /**
	     * Methods call by function
	     * @param args
	     * @throws IOException
	     */
	    public static void main(String[] args) throws IOException {

	        makeConnection();

	          System.out.println("Inserting a new Person with name Kathija...");
	          Person person = new Person();
	          person.setName("Kathija");
	          person = insertPerson(person);
	          System.out.println("Person inserted --> " + person);

	          Person personFromDB = getPersonById("25e7bc7a-7adb-4650-9f31-3d5f493bfdec");
	          System.out.println("Person from DB  --> " + personFromDB);

	          deletePersonById("09ddd7ad-581a-411c-a8c5-f319d0fac6b5");
	        
	 
	          person.setName("Basi C");
	          updatePersonById("25e7bc7a-7adb-4650-9f31-3d5f493bfdec", person);
	     

	          closeConnection();
	    }

	}


