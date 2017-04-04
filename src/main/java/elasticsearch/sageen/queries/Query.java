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
	
	 //fields
		private static String field = "profile_experience.title.raw";
		private static String rangeField = "company_rank";
			
			
		//values
		private static String idToSearch = "AVblysikw4nxuAH43H_1";
		private static String termValue = "";
		private static String matchValue = "";
		private static String matchPhraseValue = "manager";
		private static String wildCardValue = "Dir*";
		private static String prefixValue="Dir";
		private static String fuzzyValue="directar";
		//boolquery value
		private static String mustValue = "Accountant";
		private static String mustNotValue = "Account";
		private static String shouldValue = "Sales Manager";
		
			
		private static String QueryString="Sales AND Manager OR Representative";
			//boost
		private static float boostBy=0f;
		private static String boostValue = "Vice";
		private static List<String> termsValue=new ArrayList<String>();

	
	private final static int size= 50;
	private static int from=0;
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		QueryType searchType = QueryType.MATCH_ALL;

		termsValue.add("Accounting Manager");
		termsValue.add("Sales Manager");
	
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
			qb = QueryBuilders.termQuery(field, termValue);
			break;

		//for exact match terms: need not_analyzed Field
		case TERMS:
			qb = QueryBuilders.termsQuery(field, termsValue);
			break;
			
		//for match any word in term
		case MATCH:
			qb = QueryBuilders.matchQuery(field, matchValue);
			
			break;

		//for match phrase of word: need not_analyzed Field
		case MATCH_PHRASE:
			qb = QueryBuilders.matchPhraseQuery(field, matchPhraseValue);
			break;

		//for wild card search: need not_analyzed Field
		case WILDCARD:
			qb = QueryBuilders.wildcardQuery(field, wildCardValue);
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
            .negative(QueryBuilders.termQuery(field,termValue))
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
		
		//returns all datas
		case MATCH_ALL:
			qb = QueryBuilders.matchAllQuery();
			break;
			
		//incase of spelling mistake
		case FUZZY:
			Fuzziness fuzziness=null;
			qb = QueryBuilders.fuzzyQuery(field, fuzzyValue).fuzziness(fuzziness.TWO);
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
			
			System.out.println("GET "+index+"/"+type+"/_search");
			SearchRequestBuilder srbWithQuery = srb.setQuery(qb)
			.setFrom(from)
			.setSize(size);
			
			System.out.println(srbWithQuery);
			
			SearchResponse response = srbWithQuery.execute().actionGet();
			SearchHits shs = response.getHits();
			System.out.println(searchType +" hits: " + shs.getTotalHits());
			for (SearchHit sh : response.getHits().getHits()) {

					System.out.println("************************");
					System.out.println("ES score::"+sh.getScore());
					System.out.println(sh.getId());
					System.out.println("DOCUMENTS::"+sh.getSourceAsString());
					System.out.println("************************");
				
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
