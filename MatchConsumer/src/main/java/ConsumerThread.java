import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class ConsumerThread implements Runnable {

    private String exchangeName;
    private String queueName;
    private Connection connection;
    private Map<String, Set<String>> matchRecord;
    private Map<String, AtomicInteger[]> statRecord;
    private int queueSize;
    private Gson gson = new Gson();
    private static final int MATCHES_NUM = 100;
    private static final boolean DURABLE = true; // persistent queue

    public ConsumerThread(String exchangeName, String queueName, Connection connection,
                          Map<String, Set<String>> matchRecord, Map<String, AtomicInteger[]> statRecord, int queueSize) {
        this.exchangeName = exchangeName;
        this.queueName = queueName;
        this.connection = connection;
        this.matchRecord = matchRecord;
        this.statRecord = statRecord;
        this.queueSize = queueSize;
    }

    @Override
    public void run() {
        try {
            Channel channel = connection.createChannel();
            // Name, Durable (survive a broker restart), Exclusive (used by only one connection), Auto-delete, Arguments
            channel.queueDeclare(queueName, DURABLE, false, false, null);
            channel.exchangeDeclare(exchangeName, "fanout");
            channel.queueBind(queueName, exchangeName, "");
            channel.basicQos(queueSize);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                JsonObject info = this.gson.fromJson(message, JsonObject.class);
                String swiper = info.get("swiper").getAsString();
                boolean userLiked = info.get("userLiked").getAsBoolean();
                int idx = userLiked ? 0 : 1;
                if (userLiked) {
//                    if (!matchAllTime.containsKey(swiper)) {
//                        Set<String> mySet = ConcurrentHashMap.newKeySet();
//                        matchAllTime.put(swiper, mySet);
//                    }
//                    if (matchAllTime.get(swiper).size() < MATCHES_NUM) {
//                        String swipee = info.get("swipee").getAsString();
//                        if (matchAllTime.get(swiper).add(swipee)) {
//                            if (!matchRecord.containsKey(swiper)) {
//                                Set<String> mySet = ConcurrentHashMap.newKeySet();
//                                matchRecord.put(swiper, mySet);
//                            }
//                            matchRecord.get(swiper).add(swipee);
//                        }
//                    }
                    if (!matchRecord.containsKey(swiper)) {
                        Set<String> mySet = ConcurrentHashMap.newKeySet();
                        matchRecord.put(swiper, mySet);
                    }
                    matchRecord.get(swiper).add(info.get("swipee").getAsString());
                }
                if (!statRecord.containsKey(swiper)) {
                    statRecord.put(swiper, new AtomicInteger[]{new AtomicInteger(0), new AtomicInteger(0)});
                }
                statRecord.get(swiper)[idx].incrementAndGet();
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            };
            channel.basicConsume(queueName, false, deliverCallback, consumerTag -> { });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
