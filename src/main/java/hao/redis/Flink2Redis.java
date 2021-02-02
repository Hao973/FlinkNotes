package hao.redis;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// java -cp ./target/FlinkExample-1.0-SNAPSHOT-jar-with-dependencies.jar flink.demo.redis.Flink2Redis
public class Flink2Redis {
    public static void main(String[] args) throws Exception {
        System.out.println("Flink2Redis Demo");
        // get redis config
        String redisConfFile = "conf/redis.properties";
        ParameterTool parameters = ParameterTool.fromPropertiesFile(redisConfFile);
        System.out.println("redisServers:" + parameters.get("redisServers", ""));
        System.out.println("redisHost:" + parameters.get("redisHost", ""));
        System.out.println("redisPort:" + parameters.getInt("redisPort", 6379));
        System.out.println("redisPassword:" + parameters.get("redisPassword", ""));
        System.out.println("db:" + parameters.getInt("db", 0));
        // set hostname port
        String hostname = "127.0.0.1";
        int port;
        try{
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        }catch (Exception e){
            System.out.println("No port set. use default port 9000 -- java ");
            port = 9000;
        }
        System.out.println("Port: " + port);
        // 1 get flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameters);
        String delimiter = "\n";
        DataStream<Tuple2<String, String>> text = env.socketTextStream(hostname, port, delimiter)
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String s) {
                        try {
                            String[] sp = s.split("\\s");
                            return new Tuple2<>(sp[0], sp[1]);
                        }catch(Exception e){
                            return null;
                        }
                    }
                });
        text.print();
        text.addSink(new RedisSink2());
        env.execute("Flink2Redis");
    }
}
