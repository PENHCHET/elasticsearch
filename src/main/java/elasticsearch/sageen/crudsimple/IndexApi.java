package elasticsearch.sageen.crudsimple;

import java.io.IOException;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;

import elasticsearch.sageen.client.ElasticSearch;
import elasticsearch.sageen.utils.ConfigUtils;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class IndexApi {
	private static final String index = ConfigUtils.getProperty("esIndex");
	private static final String type = ConfigUtils.getProperty("esType");
	private static final Client client = ElasticSearch.CLIENT.getInstance();
	
	public static void main(String[] args) throws ElasticsearchException, IOException {
		insertByJsonBuild();
		insertByJsonString();
	}

	public static IndexResponse insertByJsonBuild() throws ElasticsearchException,
			IOException {
		IndexResponse response = client.prepareIndex(index, type,"1")
				.setSource(jsonBuilder().startObject()
								.field("name", "ram")
								.endObject()).execute().actionGet();
		System.out.println(response.isCreated());
		return response;
	}
	
	public static IndexResponse insertByJsonString()
	{
		String json = "{" +
		        "\"user\":\"ram\"," +
		        "\"postDate\":\"2017-02-07\"," +
		        "\"message\":\"ram eat rice\"" +
		    "}";

		IndexResponse response = client.prepareIndex(index, type)
		        .setSource(json)
		        .execute()
		        .actionGet();
		return response;
	}

}
