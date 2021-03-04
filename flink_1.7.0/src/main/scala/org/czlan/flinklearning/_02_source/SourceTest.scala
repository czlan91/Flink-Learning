package org.czlan.flinklearning._02_source


import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.util.Random

/**
 * 温度传感器 读数样例类
 */
case class SensorReading(id: String, timestamp: Long, temperature: Double)

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/24 15:02
 * @Description:
 * @Version: V1.0.0.0
 */
object SourceTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1);

        // 1. 从自定义集合中读取数据
        val stream1:DataStream[SensorReading] = env.fromCollection(List(
            SensorReading("sensor_1", 1547718199, 35.80018327300259),
            SensorReading("sensor_6", 1547718201, 15.40018327300259),
            SensorReading("sensor_7", 1547718202, 6.79218327300259),
            SensorReading("sensor_10", 1547718205, 38.10118327300259)
        ))

        val stream2:DataStream[String] = env.fromElements("hadoop flink spark", "hadoop spark flink")
        val streamGen:DataStream[Long] = env.generateSequence(1, 100)

        val streamSocket:DataStream[String] = env.socketTextStream("localhost", 9999)

        // 可以是 file:///  or hdfs：//
        // 可以是 文件，也可以是目录
        // 2. 从文件中读取数据
        val streamFile:DataStream[String] = env.readTextFile(" /Users/chenzhuanglan/WorkSpace/Flink_Learning/Flink_Learning/flink_1.7.0/src/main/resources/sensor.txt")


        // 3. 从kafka读取数据
        env.enableCheckpointing(5000)

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
        val stream3 = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), properties))


        // 4. 自定义source
        val stream4 = env.addSource(new SensorSource());



        // stream1.print("stream1").setParallelism(1)
        streamFile.print("stream2").setParallelism(1)

        env.execute("source test")
    }

}


class SensorSource extends SourceFunction[SensorReading] {

    // 定义一个flag，表示数据源是否正常运行
    var running: Boolean = true

    /**
     * 正常生成数据
     *
     * @param sourceContext
     */
    override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
        // 初始化一个随机数发生器
        val rand = new Random()

        // 初始化定义一组传感器温度数据
        var curTemp = 1.to(10).map(
            i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
        )



        // 用无限循环，产生数据流
        while(running){
            // 在前一次温度的基础上更新温度值
           curTemp= curTemp.map(
                t => ( t._1,t._2 + rand.nextGaussian())
            )



            // 获取当前时间戳
            val curTime = System.currentTimeMillis()
            curTemp.foreach(
                // sourceContext.collect(） 发送数据
                t => sourceContext.collect(SensorReading( t._1, curTime,t._2))
            )
            // 设置时间间隔
            Thread.sleep(1000)
        }
    }

    /**
     * 取消数据的生成
     */
    override def cancel(): Unit = {
        running = false
    }
}