package elasticsearch.sageen.client;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import elasticsearch.sageen.utils.ConfigUtils;

public enum ElasticSearch {

	CLIENT;

	private final Logger logger = LoggerFactory.getLogger(ElasticSearch.class);
	private final Client client;
	private final TransportClient transportClient;

	private ElasticSearch() {
		logger.info("Creating new elasticsearch client object...");
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", ConfigUtils.getProperty("esCluster"))
				.put("client.transport.sniff", false)
				.put("client.transport.ping_timeout", 30, TimeUnit.SECONDS).build();
				
				
		transportClient = new TransportClient(settings);
		this.client = transportClient
				.addTransportAddress(new InetSocketTransportAddress(ConfigUtils
						.getProperty("esHost"), Integer.parseInt(ConfigUtils
						.getProperty("esPort"))));
	}

	public Client getInstance() {
		return this.client;
	}

	public void destory() {
		this.client.close();
	}

}