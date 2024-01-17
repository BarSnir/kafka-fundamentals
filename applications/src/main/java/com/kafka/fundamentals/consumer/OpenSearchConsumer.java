package com.kafka.fundamentals.consumer;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchConsumer {

    private static final Logger log = LoggerFactory.getLogger(
        OpenSearchConsumer.class.getSimpleName()
    );

    public static void main(String[] args) throws IOException {
        String consumerInstance = args[0];
        RestHighLevelClient openSearchClient = createOpenSearchClient();
        KafkaConsumer<String, String> consumer = createKafkaConsumer(consumerInstance);
        addShutDownHook(consumer);
        try {
            createOSIndex(openSearchClient);
            consumer.subscribe(Collections.singleton("Wikis"));
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                log.info("Received " + recordCount + " record(s)");
                BulkRequest bulkRequest = new BulkRequest();
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        String id = extractId(record.value());
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);
                        bulkRequest.add(indexRequest);
                    } catch (Exception e){
                        log.error("Failed to add item to bulk request!");
                        log.error(e.getMessage());
                    }
                }
                if (bulkRequest.numberOfActions() > 0){
                    processRecords(openSearchClient, bulkRequest, consumer);
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            // close the consumer, this will also commit offsets
            consumer.close(); 
            openSearchClient.close();
            log.info("The consumer is now gracefully shut down");
        }

    }

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();
        if (userInfo == null) {
            // REST client without security
            return new RestHighLevelClient(
                RestClient.builder(getNoneSecureConn(connUri))
            );
        } else {
            // REST client with security
            CredentialsProvider cp = setCredentials(userInfo);
            RestClientBuilder builder = RestClient.builder(getSecureConn(connUri));
            builder = getHttpsCallback(builder, cp);
            restHighLevelClient = new RestHighLevelClient(builder);
        }
        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(String consumerInstance){
        String groupId = "consumer-opensearch-demo";
        String consumerId = consumerInstance;
        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, consumerId);
        // create consumer
        return new KafkaConsumer<>(properties);
    }

    public static void addShutDownHook(KafkaConsumer<String, String> consumer) {
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public static void createOSIndex(RestHighLevelClient openSearchClient) throws IOException {
        boolean indexExists = openSearchClient.indices()
        .exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
        if (!indexExists){
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
            openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            log.info("The Wikimedia Index has been created!");
        } else {
            log.info("The Wikimedia Index already exits");
        }
    }

    private static String extractId(String json){
        // gson library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static CredentialsProvider setCredentials(String userInfo) {
        String[] auth = userInfo.split(":");
        CredentialsProvider cp = new BasicCredentialsProvider();
        cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));
        return cp;
    }

    public static HttpHost getNoneSecureConn(URI connUri) {
        return new HttpHost(connUri.getHost(), connUri.getPort(), "http");
    }

    public static HttpHost getSecureConn(URI connUri) {
        return new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme());
    }

    public static RestClientBuilder getHttpsCallback(RestClientBuilder builder, CredentialsProvider cp) {
        return builder.setHttpClientConfigCallback(
            httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
        );
    }

    public static void processRecords (
        RestHighLevelClient openSearchClient,
        BulkRequest bulkRequest, 
        KafkaConsumer<String, String> consumer 
    ) throws IOException {
        BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        log.info("Inserted " + bulkResponse.getItems().length + " record(s).");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // commit offsets after the batch is consumed
        consumer.commitSync();
        log.info("Offsets have been committed!");
    }
}