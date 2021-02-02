package hao.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class StreamKafkaDemo {
    public static void main(String[] args) throws Exception {
        // get redis config
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","10.16.70.192:9092");
        prop.setProperty("group.id","con1");
        String topic = "WordCount";
        // /data/opt/kafka
        //./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic WordCount
        //./bin/kafka-console-consumer.sh  --bootstrap-server localhost:9092 --from-beginning --topic WordCount
        //./bin/kafka-console-consumer.sh  --bootstrap-server localhost:9092 --from-beginning --topic guo
        String outTopic = "guo";
//        String redisConfFile = "conf/kafkaConf.properties";
//        ParameterTool parameters = ParameterTool.fromPropertiesFile(redisConfFile);
//        Properties prop = parameters.getProperties();
//        String topic = parameters.get("topic");
//        System.out.println("bootstrap.servers:" + parameters.get("bootstrap.servers"));
//        System.out.println("group.id:" + parameters.get("group.id"));
//        System.out.println("topic:" + parameters.get("topic"));

        // get flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get kafka consumer
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), prop);
//        kafkaConsumer.setStartFromGroupOffsets(); //default
//        kafkaConsumer.setStartFromEarliest();
//        kafkaConsumer.setStartFromLatest();
//        kafkaConsumer.setStartFromTimestamp(1);
//        kafkaConsumer.setStartFromSpecificOffsets();
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                outTopic,
                new SimpleStringSchema(),
                prop);
        DataStream<String> text = env.addSource(kafkaConsumer);
        DataStream<String> out = text.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return "flink:" + s;
            }
        });
        out.addSink(myProducer);
        out.print().setParallelism(1);

        env.execute("StreamingKafkaDemo");
    }
}
