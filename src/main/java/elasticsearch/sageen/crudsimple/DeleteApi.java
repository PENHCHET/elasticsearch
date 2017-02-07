package elasticsearch.sageen.crudsimple;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.Client;

import elasticsearch.sageen.client.ElasticSearch;
import elasticsearch.sageen.utils.ConfigUtils;




public class DeleteApi {
	private static final String index = ConfigUtils.getProperty("esIndex");
	private static final String type = ConfigUtils.getProperty("esType");
	private static final Client client = ElasticSearch.CLIENT.getInstance();
	
	public static void deleteDocument(Client client, String id){

        DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();
        System.out.println("Information on the deleted document:");
        System.out.println("Index: " + response.getIndex());
        System.out.println("Type: " + response.getType());
        System.out.println("Id: " + response.getId());
        System.out.println("Version: " + response.getVersion());
    }
}
