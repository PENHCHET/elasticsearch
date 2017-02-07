package elasticsearch.sageen.crudsimple;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import elasticsearch.sageen.client.ElasticSearch;


public class UpdateApi {
	
	
	private static Client client=ElasticSearch.CLIENT.getInstance();
	
	public static void main(String[] args) {
		UpdateApi.updateNestedObject(client, "AVXt_hvfs_P2XQIYXzjG");
	}

	public static void update(Client client, String id, String field,
			String newValue) {

		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index("index");
		updateRequest.type("type");
		updateRequest.id(id);
		try {
			updateRequest.doc(jsonBuilder().startObject()
					.field(field, newValue).endObject());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			client.update(updateRequest).get();
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Update nested object
	 * @param client
	 * @param esId
	 * @param field
	 * @param newValue
	 */
	public  static void updateNestedObject(Client client, String esId)
	{
		XContentBuilder xcontentUpdateObj = null;
		try {
			xcontentUpdateObj = XContentFactory.jsonBuilder()
					.startObject()
			        	.startObject("socialProfiles")
			        		.startObject("linkedIn") 
			                	.field("firstName","Sageen")
			                	.field("lastName","Suwal")
			                .endObject()
			             .endObject()    
			      .endObject();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//to remove all other field
		UpdateResponse updateRequest= client.prepareUpdate("discoverleads", "cnverg", esId).setDoc(xcontentUpdateObj).execute().actionGet();
		//to update
		//UpdateResponse updateRequest= client.prepareUpdate("discoverleads", "cnverg", esId).putHeader(key, value).execute().actionGet();
		if(!updateRequest.isCreated())
		{
			System.out.println("updated");
		}
		else
		{
			System.out.println("not updated");
		}
	}

}
