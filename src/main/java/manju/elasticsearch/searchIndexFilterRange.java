package manju.elasticsearch;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.ml.GetFiltersRequest;
import org.elasticsearch.client.ml.GetFiltersResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.opencsv.CSVWriter;

public class searchIndexFilterRange {
	
	private static String[] strArr;

	public static void main(String[] args) throws Exception {
		
		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost("localhost", 9200, "http")));
	
					// Common search and search by index
		
//		SearchRequest searchRequest = new SearchRequest(); 
//		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
//		searchSourceBuilder.query(QueryBuilders.matchAllQuery()); 
//		searchRequest.source(searchSourceBuilder);
//		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
//		SearchHits hits = searchResponse.getHits();
//		SearchHit[] searchHits = hits.getHits();
//		for (SearchHit hit : searchHits) {
//			System.out.println(hit.getSourceAsString());
//		}
//		
//		
//						//  search by field
//		
//		SearchRequest searchRequest1 = new SearchRequest(); 
//	    MatchQueryBuilder matchQueryBuilder1 = new MatchQueryBuilder("name", "Saju"); 
//		System.out.println(matchQueryBuilder1);
//		SearchSourceBuilder searchSourceBuilder1 = new SearchSourceBuilder(); 
//		searchSourceBuilder1.query(matchQueryBuilder1); 
//		searchRequest1.source(searchSourceBuilder1);
//		SearchResponse searchResponse11 = client.search(searchRequest1, RequestOptions.DEFAULT);
//		SearchHits hits11 = searchResponse11.getHits();
//		SearchHit[] searchHits11 = hits11.getHits();
//		for (SearchHit hit : searchHits11) {
//			String sourceAsString = hit.getSourceAsString();
//            System.out.println(sourceAsString);
//
//		}
		
		
		//search by template
		
		SearchTemplateRequest request = new SearchTemplateRequest();
		request.setRequest(new SearchRequest("registrationdata")); 

		request.setScriptType(ScriptType.INLINE);
		request.setScript( 
		    "{" +
		    "  \"query\": { \"match\" : { \"{{field}}\" : \"{{value}}\" } }" +
		    "}");
		Map<String, Object> scriptParams = new HashMap<>();
		scriptParams.put("field", "first_name");
		scriptParams.put("value", "Manju");
		request.setScriptParams(scriptParams);
		SearchTemplateResponse response = client.searchTemplate(request, RequestOptions.DEFAULT);
		SearchResponse searchResponse12=response.getResponse();
		SearchHits hits12 = searchResponse12.getHits();
		SearchHit[] searchHits12 = hits12.getHits();
		for (SearchHit hit12 : searchHits12) {
			Map<String, Object> sourceAsMap = hit12.getSourceAsMap();		
			String lanme = (String) sourceAsMap.get("last_name");
			String fanme = (String) sourceAsMap.get("first_name");
			Integer id = (Integer) sourceAsMap.get("id");
			Integer age = (Integer) sourceAsMap.get("age");
			String idString=id.toString();
			String ageString=age.toString();
		
		        System.out.println((id));
		        String[] strArray1 = new String[4];
		        strArray1[0]="id";
		        strArray1[1]="First_name";
		        strArray1[2]="last_name";
		        strArray1[3]="age";
		        
		        String[] strArray = new String[4];
		        strArray[0]=idString;
		        strArray[1]=fanme;
		        strArray[2]=lanme;
		        strArray[3]=ageString;


		        System.out.println(Arrays.toString(strArray));
			 CSVWriter csvWriter = new CSVWriter(new FileWriter("//home//e1066//Documents//JS//JS task - aug22//myfile.csv"));
	            csvWriter.writeNext(strArray1);
	            csvWriter.writeNext(strArray);
			  csvWriter.close();

            }
				
		
		//search by filter range
		
//		SearchSourceBuilder builder = new SearchSourceBuilder()
//				  .postFilter(QueryBuilders.rangeQuery("age").from(20).to(25));
//
//				SearchRequest searchRequest13 = new SearchRequest();
//				searchRequest13.searchType(SearchType.DFS_QUERY_THEN_FETCH);
//				searchRequest13.source(builder);
//
//				SearchResponse response13 = client.search(searchRequest13, RequestOptions.DEFAULT);
//				SearchHits hits13 = response13.getHits();
//				SearchHit[] searchHits13 = hits13.getHits();
//				for (SearchHit hit : searchHits13) {
//					String sourceAsString = hit.getSourceAsString();
//		            System.out.println(sourceAsString);
//		
//				}
	}
}
