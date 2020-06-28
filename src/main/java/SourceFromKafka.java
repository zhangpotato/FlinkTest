import com.alibaba.fastjson.JSONObject;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Properties;

public class SourceFromKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.133.140:9092,192.168.133.141:9092");
        properties.put("group.id", "flink");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "latest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<String>(KafkaWrite.TOPIC_FLINK, new SimpleStringSchema(), properties)).setParallelism(1);
        DataStream<Person> dataStream = dataStreamSource.map(value -> JSONObject.parseObject(value, Person.class));
        dataStream.timeWindowAll(Time.seconds(5L)).apply(new AllWindowFunction<Person, List<Person>, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<Person> iterable, Collector<List<Person>> collector) throws Exception {
                List<Person> persons = Lists.newArrayList(iterable);

                if (persons.size() > 0) {
                    System.out.println("5秒的总共收到的条数：" + persons.size());
                    collector.collect(persons);
                }
            }
        }).addSink(new DBSink());

        env.execute("开始获取Kafka数据");
    }
}
