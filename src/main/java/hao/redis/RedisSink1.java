package hao.redis;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.util.Random;

public class RedisSink1 extends RichSinkFunction<Tuple2<String, String>> {
    private transient Jedis jedis;

    @Override
    public void open(Configuration config) {
        final String redisConfInfo = "10.16.70.192:6379,10.16.70.192:6379,10.16.70.192:6379,10.16.70.192:6379";
        final String redisPassword = "PEJlpxSiA2vg";

        String[] redisConfList = redisConfInfo.split(",");
        int randomNum = (new Random()).nextInt(redisConfList.length);
        String randomConf = redisConfList[randomNum];
        String redisHost = randomConf.split(":")[0];
        int redisPort = Integer.parseInt(randomConf.split(":")[1]);
        jedis = new Jedis(redisHost, redisPort);
//        System.out.println("connect redis[" + redisHost + ": " + redisPort + "] ok");
        jedis.auth(redisPassword);
    }


    @Override
    public void invoke(Tuple2<String, String> value, Context context) throws Exception {
        if (!jedis.isConnected()) {
            jedis.connect();
        }

        //保存
        jedis.setex(value.f0, 24*3600, value.f1);
    }

    @Override
    public void close() throws Exception {
        jedis.close();
    }
}
