package db;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import constant.Constant;

public class MongoManager {

    private static MongoClient mongoClient;
    private static MongoManager mongoManager;

    private MongoManager() {
        ConnectionString connectionString = new ConnectionString(Constant.MONGO_URL);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .serverApi(ServerApi.builder()
                        .version(ServerApiVersion.V1)
                        .build())
                .build();
        mongoClient = MongoClients.create(settings);
    }

    public static MongoClient getClient() {
        if (mongoManager == null) {
            mongoManager = new MongoManager();
        }
        return mongoClient;
    }
}
