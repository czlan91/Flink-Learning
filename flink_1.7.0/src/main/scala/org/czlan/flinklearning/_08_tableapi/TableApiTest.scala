package org.czlan.flinklearning._08_tableapi

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala.StreamTableEnvironment


/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/25 21:26
 * @Description:
 * @Version: V1.0.0.0
 */
object TableApiTest {
    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1);


        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "localhost:9092")
        properties.setProperty("group.id", "consumerr-group")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "latest")

        /**
         * 在spark中，为了确保 exactly once，需要我们手动的取管理 kafka的 offset，
         * 但是在 flink中，FlinkKafkaConsumer 已经帮我们做好了，
         */
        val stream = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), properties))

        val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

        val ecommerceLogDstream: DataStream[EcommerceLog] = stream
                .map(jsonString => JSON.parseObject(jsonString, classOf[EcommerceLog]))
//                .map(ecommercelog => (ecommercelog.mid, ecommercelog.ch))

        val ecommerceLogTable: Table = tableEnv.fromDataStream(ecommerceLogDstream)

        val table: Table = ecommerceLogTable.select("mid,ch").filter("ch='appstore'")

        val midchDataStrem: DataStream[(String, String)] = tableEnv.toAppendStream[(String, String)](table)

        midchDataStrem.print()
        env.execute()


    }

}

case class EcommerceLog(mid: String, ch: String);