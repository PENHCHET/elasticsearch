package elasticsearch.sageen.queries;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import elasticsearch.sageen.client.ElasticSearch;
import elasticsearch.sageen.utils.ConfigUtils;

public class Query {

	private static final String index =ConfigUtils.getProperty("esIndex");
	private static final String type =ConfigUtils.getProperty("esType");
	private static final Client client = ElasticSearch.CLIENT.getInstance();

	private static String field = "";
	private static String value = "";
	private static String idToSearch = "";
	private static String singleField = "";
	private static String mustValue = "";
	private static String mustNotValue = "";
	private static String shouldValue = "";
	private static String rangeField = "";
	private static String QueryString="";
	private static float boostBy=0f;
	private static String boostValue = "";
	private static String prefixValue="";
	private static List<String> values;
	private final static int size= 50;
	private static int from;
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		QueryType searchType = QueryType.MATCH_PHRASE;
		
		//fields
		rangeField= "company_rank";
		field = "FIELD_NAME.SUB_FILED.lower_case_sort";
		
		//values:
		//for Term , match query, match phrase query
		value = "Esta loca!";
		
		//prefix
		prefixValue= "Dir";
		
		//for search By ID
		idToSearch = "AVblysikw4nxuAH43H_1";
		
		//for termsQuery
		values = new ArrayList<String>();
		values.add("Accounting Manager");
		values.add("Sales Manager");
		
		//for bool query must and mustnot and should
		mustNotValue = "Accounting Manager";
		mustValue = "Marketing Manager";
		shouldValue = "Sales Manager";
		
		//for single field return
		singleField = "FIELD_NAME";
		
		// BOOST Query
		boostBy = 0.1f;
		boostValue= "sales Manager";
		
		//Query String Query
		QueryString = "Sales AND Manager OR Representative";
		
	
		//search
		Query.search(searchType);
	
	}
	
	private static void search(QueryType searchType) {
		boolean needExecution =true;
		SearchRequestBuilder srb = client.prepareSearch(index).setTypes(type);
		QueryBuilder qb = null;
		switch (searchType) {
		
		//for exact match term: need not_analyzed Field
		case TERM:
			qb = QueryBuilders.termQuery(field, value);
			break;

		//for exact match terms: need not_analyzed Field
		case TERMS:
			qb = QueryBuilders.termsQuery(field, values);
			break;
			
		//for match any word in term
		case MATCH:
			qb = QueryBuilders.matchQuery(field, value);
			// With Queries
			//qb = QueryBuilders.constantScoreQuery(QueryBuilders.matchQuery(field,value)).boost(2.0f);
			
			break;

		//for match phrase of word: need not_analyzed Field
		case MATCH_PHRASE:
			qb = QueryBuilders.matchPhraseQuery(field, value);
			break;

		//for wild card search: need not_analyzed Field
		case WILDCARD:
			qb = QueryBuilders.wildcardQuery(field, "*"+value+"*");
			break;
			
		case SEARCH_BY_ID:
			GetRequestBuilder response = client.prepareGet(index,type, idToSearch );

			GetResponse a = response.get();
			System.out.println(a.getId());
			System.out.println(a.getSourceAsString());
			needExecution=false;
			break;
			
		//multiple condition query/combining multiple query
		case BOOL_QUERY:
			BoolQueryBuilder boolQuery = new BoolQueryBuilder();
			boolQuery.must(QueryBuilders.termQuery(field, mustValue));
			boolQuery.mustNot(QueryBuilders.termQuery(field, mustNotValue));
			boolQuery.should(QueryBuilders.termQuery(field, shouldValue));
			
			//srb.setQuery(boolQuery); //can be done remember
			qb = boolQuery;
			break;
			
		//boost search using specific filed
		case BOOST:
			qb = QueryBuilders.boostingQuery()
            .positive(QueryBuilders.termQuery(field,boostValue))
            .negative(QueryBuilders.termQuery(field,value))
            .negativeBoost(boostBy)
            .boost(boostBy);  
			break;
			
		case QUERY_STRING_QUERY:
				
			// qb = QueryBuilders.queryStringQuery("+Sales +Manager -Accountant").field(field);
			 qb = QueryBuilders.queryStringQuery(QueryString).field(field);
			break;
			
		case RANGE_QUERY:
			 qb = QueryBuilders.rangeQuery(rangeField)   
		    .gt(5)                            
		    .lte(10);  
			
			break;
		//return single field	
		case GET_SINGLE_FIELD:
			srb.addFields(singleField);
			qb = QueryBuilders.matchAllQuery();
			break;
		
		//returns all datas
		case MATCH_ALL:
			qb = QueryBuilders.matchAllQuery();
			break;
			
		//incase of spelling mistake
		case FUZZY:
			Fuzziness fuzziness=null;
			qb = QueryBuilders.fuzzyQuery(field, value).fuzziness(fuzziness.TWO);
			break;
			
		//prefix value query: need not_analyzed Field
		case PREFIX:
		
			qb =QueryBuilders.prefixQuery(field, prefixValue);
			break;

		default:
			qb = QueryBuilders.matchAllQuery();
			break;
		}
		long startTime = System.currentTimeMillis();
		
		if(needExecution)
		{
			from=0;
			System.out.println("GET "+index+"/"+type+"/_search");
			System.out.println(srb.setQuery(qb).setFrom(from).setSize(size));
			SearchResponse response = srb.setQuery(qb)
					.setFrom(from)
					.setSize(size)
					.execute().actionGet();
			//srb.setMinScore(0.5)
			SearchHits shs = response.getHits();
			System.out.println(searchType +" hits: " + shs.getTotalHits());
			for (SearchHit sh : response.getHits().getHits()) {
				if(searchType==QueryType.GET_SINGLE_FIELD)
				{
					System.out.println(sh.getSourceAsString());
					System.out.println(sh.field("fieldName").getValue().toString());
				}
				else
				{
					System.out.println("************************");
					System.out.println("ES score::"+sh.getScore());
					System.out.println(sh.getId());
					System.out.println("DOCUMENTS::"+sh.getSourceAsString());
					System.out.println("************************");
				}
			}
		}
		long endTime = System.currentTimeMillis();
		timeCalcutation(startTime, endTime);
	}

	private static void timeCalcutation(long startTime, long endTime) {
		System.out.println("***********************************************");
		System.out.println("Time taken in millisecond: "
				+ (endTime - startTime));
		System.out
				.println("Time taken in sec: " + (endTime - startTime) / 1000);
		System.out.println("Time taken in min: " + (endTime - startTime)
				/ 60000);
		System.out.println("************************************************");
		System.out.println();

	}

}
