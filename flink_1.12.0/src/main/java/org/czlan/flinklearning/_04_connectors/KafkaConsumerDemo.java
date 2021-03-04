package org.czlan.flinklearning._04_connectors;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description Flink-Connectors-KafkaConsumer/Source
 *
 * 建议设置的项：
 * 1. 订阅的主题
 * 2. 反序列化规则
 * 3. 消费者属性-集群地址
 * 4. 消费者属性-消费者组id（如果不设置，会有默认的，但是默认的不方便管理）
 * 5. 消费者属性-offset重置规则，如earlieast/latest
 * 6. 动态分区检测（当kafka的分区数变化，增加时，Flink能够检测到！）
 * 7. 如果没有设置checkpoint，那么可以设置提交offset，后续学习了checkpoint会把offset随着做checkpoint的时候提交到checkpoint和默认主题中。
 * @date 2021/2/20 19:51
 */
public class KafkaConsumerDemo {
    public static void main(String[] args) throws Exception {
        // todo 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // todo 1.source
        // 准备kafka连接参数
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","localhost:9092");
        props.setProperty("group.id","flink");
        // latest 有offset记录，从记录位置开始消费，没有记录，从最新的/最后的消息开始消费
        // ealiest 有offset记录，从记录位置开始消费，没有记录，从最早的/最开始的消息开始消费
        props.setProperty("auto.offset.reset","latest");
        // 会开启一个后台线程每隔5s检测一下kafka的分区情况
        props.setProperty("flink.partition-discovery.interval-millis","5000");
        //  自动提交到 默认主题，后续会存储在checkpoint和默认主题中
        props.setProperty("enable。auto.commit","true");
        props.setProperty("auto.commit.interval.ms","2000");


        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("flink_kafka", new SimpleStringSchema(), props);

        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        // todo 2.transformation

        // todo 3.sink
        kafkaDS.print();

        // todo 4.execute
        env.execute();
    }
}
// 启动kafka
// 创建topic  bin/kafka-topics.sh --zookeeper zookeeper:2181 --describe --topic flink_kafka
// 启动生产者生产数据
// 启动flink程序