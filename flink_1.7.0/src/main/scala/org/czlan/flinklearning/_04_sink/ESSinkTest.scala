package org.czlan.flinklearning._04_sink

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.czlan.flinklearning._02_source.SensorReading
import org.elasticsearch.client.Requests

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/24 18:58
 * @Description:
 * @Version: V1.0.0.0
 */
object ESSinkTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)


        val streamFromFile = env.readTextFile("/Users/chenzhuanglan/OneDrive/learning/WorkSpace/Flink_Learning/src/main/resources/sensor.txt")

        // 转换为String 方便输出
        val dataStream: DataStream[SensorReading] = streamFromFile
                .map(
                    data => {
                        val dataArray = data.split(",")
                        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
                    }
                )

        // sink
        val httpHosts = new util.ArrayList[HttpHost]
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))
        httpHosts.add(new HttpHost("10.2.3.1", 9200, "http"))

        // 创建一个 esSink的Builder
        val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
            httpHosts,
            new ElasticsearchSinkFunction[SensorReading] {

                override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
                    // 包装成一个Json 或者 Object
                    println("saving data" + t)
                    val json = new util.HashMap[String, String]()
                    json.put("sensor_id", t.id)
                    json.put("temperature", t.temperature.toString)
                    json.put("ts", t.timestamp.toString)
                    // 创建 Index request，准备发送数据
                    val indexRequest = Requests.indexRequest()
                            .index("sensor")
                            .`type`("readingdata")
                            .source(json)

                    // 利用 requestIndexer发送请求
                    requestIndexer.add(indexRequest)
                    println("data saved....")

                }
            }
        )


        dataStream.addSink(esSinkBuilder.build())
        env.execute("es sink test")
    }
}

