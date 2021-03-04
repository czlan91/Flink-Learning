package org.czlan.flinklearning.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.typeutils.runtime.kryo.Serializers.DummyAvroKryoSerializerClass
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/26 12:48
 * @Description:
 * @Version: V1.0.0.0
 */
object MyKafkaUtil {


    def getConsumer(topic:String): FlinkKafkaConsumer[String] ={
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "localhost:9092")
        properties.setProperty("group.id", "consumerr-group")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "latest")

        new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
    }

    def writeToKafka(topic:String): Unit ={
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "localhost:9092")
        properties.setProperty("key.Serializer", "org.apache.kafka.common.serialization.StringSerializer")
        properties.setProperty("value.Serializer", "org.apache.kafka.common.serialization.StringSerializer")

        // 定义一个kafka producer
        val producter = new KafkaProducer[String,String](properties)
        // 从文件中读取数据，发送
        val bufferSource = io.Source.fromFile("/Users/chenzhuanglan/OneDrive/learning/WorkSpace/Flink_Learning/src/main/resources/UserBehavior.csv")
        for (line <- bufferSource.getLines()){
            val record  = new ProducerRecord[String,String](topic,line)
            producter.send(record)
        }

        producter.close()
    }



}