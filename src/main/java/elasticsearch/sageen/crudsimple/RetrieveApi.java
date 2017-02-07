package elasticsearch.sageen.crudsimple;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import elasticsearch.sageen.client.ElasticSearch;
import elasticsearch.sageen.utils.ConfigUtils;

public class RetrieveApi {

	private static final String index = ConfigUtils.getProperty("esIndex");
	private static final String type = ConfigUtils.getProperty("esType");
	private static final Client client = ElasticSearch.CLIENT.getInstance();
	
	public static void main(String[] args) {

		QueryBuilder qb = QueryBuilders.matchAllQuery();
		SearchRequestBuilder srb = client.prepareSearch(index).setTypes(type);
		SearchResponse response = srb.setQuery(qb)
				.setFrom(0)
				.setSize(25)
				.execute().actionGet();
		
		SearchHits shs = response.getHits();
		System.out.println(" hits: " + shs.getTotalHits());
		for (SearchHit sh : response.getHits().getHits()) {
				System.out.println("************************");
				System.out.println("ES score::"+sh.getScore());
				System.out.println(sh.getId());
				System.out.println(sh.getSourceAsString());
				System.out.println("************************");
			
		}
	}

}
