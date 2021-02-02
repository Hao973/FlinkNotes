package hao.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class StreamKafkaCheckPointDemo {
    public static void main(String[] args) throws Exception {
        // get redis config
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","10.16.70.192:9092");
        prop.setProperty("group.id","con1");
        String topic = "WordCount";
        // get flink stream execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set checkpoint
        //checkpoint默认关闭，开启checkpoint，并设置周期为1分钟
        env.enableCheckpointing(60000);
        //设置处理数据形式为exactly-once
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //检查点间隔至少为500ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //检查点必须在1分钟内完成，否则被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(5*60*1000);
        //同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //表示flink程序被cancel时，会自动保留checkpoint数据以便恢复（外部持久化)
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //表示当checkpoint发生任何异常时，直接fail该task
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);

        //设置checkpoint存储方式及存储路径，若不设置fs或rock，默认存储在把checkpoint存储在taskmanager里
//        String checkPointPath = "viewfs://cluster11/user/data/kafka_checkpoint/";
//        env.setStateBackend(new RocksDBStateBackend(checkPointPath));

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), prop);
        DataStream<String> text =  env.addSource(myConsumer).map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                Date dNow = new Date( );
                SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                return ft.format(dNow) + ": " + s;
            }
        });
        text.print().setParallelism(1);
        env.execute("StreamKafkaCheckPoint");
    }
}
