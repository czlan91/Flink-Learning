package org.czlan.flinklearning._04_connectors;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description Flink-Connectors-三方提供的RedisSink
 * https://bahir.apache.org/docs/flink/current/flink-streaming-redis/
 *
 * redis的key之中是string，value可以是 string/hash/list/set/有序set
 * 一般在使用的时候，采用 key是一个string，value是一个hash来维护数据。
 * 单词:数量 -> (key-string,value-string) 这种设计会导致很多的key，不方便维度
 * wcresult: 单词:数量 -> (key-string, value-hash) 方便维护
 * @date 2021/2/20 19:51
 */
public class RedisDemo {
    public static void main(String[] args) throws Exception {
        // todo 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // todo 1.source
        DataStream<String> lines = env.socketTextStream("localhost", 9999);

        // todo 2.transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] arr = value.split(" ");
                for (String word :
                        arr) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(t -> t.f0).sum(1);


        // todo 3.sink
        result.print();

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").build();

        RedisSink<Tuple2<String, Integer>> redisSink = new RedisSink<Tuple2<String, Integer>>(conf,new MyRedisMapper());
        result.addSink(redisSink);


        // todo 4.execute
        env.execute();
    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String,Integer>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            // 选择数据结构 对应的 key:String,value:hash(单词，数量），命令为hset
            return new RedisCommandDescription(RedisCommand.HSET,"wcresult");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> t) {
            return t.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> t) {
            return t.f1.toString();
        }
    }
}
