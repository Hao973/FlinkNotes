package hao.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


//
public class SocketWindowWordCountJava {
    public static void main(String[] args) throws Exception {
        // get port
        int port;
        try{
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        }catch (Exception e){
            System.out.println("No port set. use default port 9000 -- java ");
            port = 9000;
        }
        System.out.println("Port: " + port);

        // get flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String hostname = "127.0.0.1";
        String delimiter = "\n";
        DataStream<String> text = env.socketTextStream(hostname, port, delimiter);
        DataStream<WordWithCount> windowCounts =  text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
                String[] splits = s.split("\\s");
                for(String word: splits){
                    collector.collect(new WordWithCount(word, 1L));
                }
            }
        }).keyBy("word")
                .timeWindow(Time.seconds(2), Time.seconds(1))
                .sum("count");
        windowCounts.print().setParallelism(1);
        env.execute("Socket window count");
    }
}
