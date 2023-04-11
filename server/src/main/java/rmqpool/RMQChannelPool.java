package rmqpool;

import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import constant.*;


/**
 * A simple RabbitMQ channel pool based on a BlockingQueue implementation
 *
 */
public class RMQChannelPool {

    // used to store and distribute channels
    private final BlockingQueue<Channel> pool;
    // fixed size pool
    private int capacity;
    // used to create channels
    private RMQChannelFactory factory;


    public RMQChannelPool(int maxSize, RMQChannelFactory factory) {
        this.capacity = maxSize;
        pool = new LinkedBlockingQueue<>(capacity);
        this.factory = factory;
        for (int i = 0; i < capacity; i++) {
            Channel chan;
            try {
                chan = factory.create();
                chan.exchangeDeclare(Constant.EXCHANGE_NAME, "fanout");
                pool.put(chan);
            } catch (IOException | InterruptedException ex) {
                Logger.getLogger(RMQChannelPool.class.getName()).log(Level.SEVERE, null, ex);
            }

        }
    }

    public Channel borrowObject() throws IOException {

        try {
            return pool.take();
        } catch (InterruptedException e) {
            throw new RuntimeException("Error: no channels available" + e.toString());
        }
    }

    public void returnObject(Channel channel) {
        if (channel != null) {
            pool.add(channel);
        }
    }

//    public void close() {
//        // pool.close();
//    }
}
