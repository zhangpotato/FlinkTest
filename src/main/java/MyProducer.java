import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MyProducer {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.133.140:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
//        props.put("group.id", "test2");
        props.put("linger.ms", 1);
//        props.put("partitioner.class", "com.example.demo.MyPartitioner");
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            TimeUnit.SECONDS.sleep(2);
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i)));
            System.out.println(i);
        }
        producer.close();

    }
}