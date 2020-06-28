import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaWrite {
    public static final String BROKER_LIST = "192.168.133.140:9092,192.168.133.141:9092";
    public static final String TOPIC_FLINK = "flink";

    //key序列化的方式，采用字符串的形式
    public static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    //value的序列化的方式
    public static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public static void writeToKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("key.serializer", KEY_SERIALIZER);
        props.put("value.serializer", VALUE_SERIALIZER);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        int num = RandomUtils.nextInt(1, 100);
        Person person = new Person();
        person.setName("flink" + num);
        person.setAge(num);
        person.setdate(new Date());

        String personJson = JSON.toJSONString(person);

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_FLINK, null, null, personJson);

        producer.send(record);
        System.out.println("写入Kafka数据" + personJson);
        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        for (int i = 0; i <= 100; i++) {
            TimeUnit.SECONDS.sleep(2);
            writeToKafka();
        }
    }

}
