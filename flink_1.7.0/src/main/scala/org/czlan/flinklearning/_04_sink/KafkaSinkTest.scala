package org.czlan.flinklearning._04_sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.czlan.flinklearning._02_source.SensorReading

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/24 18:22
 * @Description:
 * @Version: V1.0.0.0
 */
object KafkaSinkTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)


        val streamFromFile = env.readTextFile("/Users/chenzhuanglan/OneDrive/learning/WorkSpace/Flink_Learning/src/main/resources/sensor.txt")

        // 转换为String 方便输出
        val dataStream: DataStream[String] = streamFromFile
                .map(
                    data => {
                        val dataArray = data.split(",")
                        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString
                    }
                )

        // sink

        dataStream.addSink(new FlinkKafkaProducer[String]("localhosts;9092","sinkTest",new SimpleStringSchema()))


        env.execute("kafka sink test")

    }

}