package hao.redis;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

public class RedisSink2 extends RichSinkFunction<Tuple2<String, String>> {
    private transient Jedis jedis;

    @Override
    public void open(Configuration config) {
        ParameterTool parameters = (ParameterTool)getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        final String redisHost = parameters.get("redisHost");
        final int redisPort = parameters.getInt("redisPort");
        final String redisPassword = parameters.get("redisPassword");
        final int db = parameters.getInt("db", 0);
        jedis = new Jedis(redisHost, redisPort);
        jedis.auth(redisPassword);
        jedis.select(db);
    }


    @Override
    public void invoke(Tuple2<String, String> value, Context context) throws Exception {
        if (!jedis.isConnected()) {
            jedis.connect();
        }
        String lock = "lock" + value.f0;
        try{
            while(1 != jedis.setnx(lock, String.valueOf(1))){
                Thread.sleep(100);
            }
            jedis.expire(lock, 10);
            String val = jedis.get(value.f0);
            if(val != null){
                jedis.setex(value.f0, 24*3600, val + "," + value.f1);
            }else{
                jedis.setex(value.f0, 24*3600, value.f1);
            }
        }finally{
            jedis.del(lock);
        }
    }

    @Override
    public void close() throws Exception {
        jedis.close();
    }
}
