import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Indexes;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import static com.mongodb.client.model.Filters.eq;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.conversions.Bson;
import org.bson.Document;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;

public class Consumer {

    private static final String EXCHANGE_NAME = "swipeExchange";
    private static final String QUEUE_NAME = "matchQueue";
    private static final int QUEUE_SIZE = 10;
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final int PORT = 5672;
    private static final int THREAD_NUM = 50;
    private static final long PERIOD = 1000;
    private static final String CONNECTION_STRING = "mongodb://172.31.26.78:27017,172.31.26.21:27017,172.31.19.185:27017/?replicaSet=rs1";

    public static void main(String[] args) throws IOException, TimeoutException {
//        String host = args[0];
        String host = "ec2-54-201-94-155.us-west-2.compute.amazonaws.com";

        // connect to RMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(PORT);
        factory.setUsername(USER);
        factory.setPassword(PASSWORD);
        Connection connection = factory.newConnection();

        // connect to mongo
        ConnectionString connectionString = new ConnectionString(CONNECTION_STRING);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .serverApi(ServerApi.builder()
                        .version(ServerApiVersion.V1)
                        .build())
                .build();
        MongoClient mongoClient = MongoClients.create(settings);
        MongoDatabase database = mongoClient.getDatabase("swipedata");

        // potential matches during one updating period
        Map<Integer, Set<String>> matchRecord = new ConcurrentHashMap<>();
        // like and dislike numbers during one updating period
        Map<Integer, AtomicInteger[]> statRecord = new ConcurrentHashMap<>();
        for (int i = 0; i < THREAD_NUM; i++) {
            Thread thread = new Thread(new ConsumerThread(EXCHANGE_NAME, QUEUE_NAME, connection, matchRecord, statRecord, QUEUE_SIZE));
            thread.start();
        }

        updateDB(database, matchRecord, statRecord);
    }

    /** connect to the db and update documents every PERIOD millisecond */
    private static void updateDB(MongoDatabase database, Map<Integer, Set<String>> matchRecord, Map<Integer, AtomicInteger[]> statRecord) {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(new repeatedTask(database, matchRecord, statRecord), 0, PERIOD, TimeUnit.MILLISECONDS);
    }

    static class repeatedTask implements Runnable {
        private MongoDatabase database;
        private Map<Integer, Set<String>> matchRecord;
        private Map<Integer, AtomicInteger[]> statRecord;

        public repeatedTask(MongoDatabase database, Map<Integer, Set<String>> matchRecord, Map<Integer, AtomicInteger[]> statRecord) {
            this.database = database;
            this.matchRecord = matchRecord;
            this.statRecord = statRecord;
        }

        @Override
        public void run() {
            if (statRecord.isEmpty()) return;
            Map<Integer, AtomicInteger[]> tempStat = new ConcurrentHashMap<>(statRecord);
            Map<Integer, Set<String>> tempMatch = new ConcurrentHashMap<>(matchRecord);
            statRecord.clear();
            matchRecord.clear();
            MongoCollection<Document> collection = database.getCollection("stats");
            for (int swiper : tempStat.keySet()) {
                Bson filter = Filters.eq("_id", swiper);
                Bson update = Updates.combine(Updates.inc("numLlikes", tempStat.get(swiper)[0]),
                        Updates.inc("numDislikes", tempStat.get(swiper)[1]));
                UpdateOptions options = new UpdateOptions().upsert(true);
                collection.updateOne(filter, update, options);
            }
            collection = database.getCollection("matches");
            for (int swiper : tempMatch.keySet()) {
                Bson filter = Filters.eq("_id", swiper);
                Bson update = Updates.addEachToSet("matchList", new ArrayList<>(tempMatch.get(swiper)));
                UpdateOptions options = new UpdateOptions().upsert(true);
                collection.updateOne(filter, update, options);
            }
        }
    }
}
