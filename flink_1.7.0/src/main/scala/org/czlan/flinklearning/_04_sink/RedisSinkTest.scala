package org.czlan.flinklearning._04_sink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.czlan.flinklearning._02_source.SensorReading

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/24 18:33
 * @Description:
 * @Version: V1.0.0.0
 */
object RedisSinkTest {
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
        val conf = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build()

        val sink = new RedisSink[SensorReading](conf, new MyRedisMapper())
        dataStream.addSink(sink)
        env.execute("redis sink test")
    }

}

class MyRedisMapper() extends RedisMapper[SensorReading] {
    // 定义保存数据到redis的命令
    override def getCommandDescription: RedisCommandDescription = {
        //  把 传感器id和温度值保存成哈希表   HSET key field value
        new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
    }

    // 定义 保存到redis的key
    override def getKeyFromData(t: SensorReading): String = t.temperature.toString

    // 定义 保存到redis的value
    override def getValueFromData(t: SensorReading): String = t.id
}