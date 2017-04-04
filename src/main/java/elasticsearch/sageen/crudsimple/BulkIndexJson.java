package elasticsearch.sageen.crudsimple;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import elasticsearch.sageen.client.ElasticSearch;


public class BulkIndexJson {

	
	public static void main(String[] args) {
		String newIndex = "bulkindex";
		String newType = "bulktype";
		String READ_JSON_FILE = "/home/user/workspace/FuseBigData/dataDump/data-dump1.json";
		Client client = ElasticSearch.CLIENT.getInstance();
		indexBulk(newIndex,newType,READ_JSON_FILE,client);
	}

	public static void indexBulk(String newIndex, String newType, String filePath, Client client)
	{
		System.out.println(newIndex+"/"+newType);
		LineIterator it = null;
		File file = new File(filePath);
		try {
			it = FileUtils.lineIterator(file, "UTF-8");
		} catch (IOException e) {
			e.printStackTrace();
		}

		BulkProcessor bulkProcessor = BulkProcessor
				.builder(client, new BulkProcessor.Listener() {

					@Override
					public void afterBulk(long arg0, BulkRequest arg1,
							Throwable arg2) {
						// TODO Auto-generated method stub

					}

					@Override
					public void beforeBulk(long executionId, BulkRequest request) {
						System.out.println(request.numberOfActions());
					}

					@Override
					public void afterBulk(long executionId,
							BulkRequest request, BulkResponse response) {
						if (response.hasFailures()) {
							System.out.println("------------------");
						}
					}

				}).setBulkActions(1000)			// max doc per action
				.setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))	// max byte size per action
				.setFlushInterval(TimeValue.timeValueSeconds(3))
				.setConcurrentRequests(1).build();

		while (it.hasNext()) {
			String line = it.nextLine();
			//System.out.println(line);
			bulkProcessor.add(new IndexRequest(newIndex, newType).source(line));
		}

	}
}


