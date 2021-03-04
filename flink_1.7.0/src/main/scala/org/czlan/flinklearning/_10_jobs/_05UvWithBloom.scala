package org.czlan.flinklearning._10_jobs

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/26 15:14
 * @Description: 需要使用redis做 bloom过滤器
 * @Version: V1.0.0.0
 */
// 定义输入数据的样例类
case class UserBehavior5(UserId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)


case class UvCount5(windowEnd: Long, UvCount: Long)

object UvWithBloom {
    def main(args: Array[String]): Unit = {
        // 1. 创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 用相对路径
        val resource = getClass.getResource("/UserBehavior.csv")

        // 2. 读取数据
        val dataStream = env.readTextFile(resource.getPath)
                .map(data => {
                    val dataArray = data.split(",")
                    UserBehavior3(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
                })
                .assignAscendingTimestamps(_.timestamp * 1000L)

        // 3. transform 处理数据
        val processedStream = dataStream
                .filter(_.behavior == "pv")
                .map(data => ("dummyKey", data.UserId))
                .keyBy(_._1)
                .timeWindow(Time.hours(1))
                .trigger(new MyTrigger())
                .process(new UvCountWithBloom())


        // 4. sink 控制台输出
        processedStream.print()

        env.execute("uv job")

    }

}

// 自定义窗口触发器
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        // 每来一条数据，就直接出发窗口操作，并清空所有窗口状态
        TriggerResult.FIRE_AND_PURGE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE


    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}

// 定义一个布隆过滤器
class Bloom(size: Long) {
    // 位图的总大小
    private val cap = if (size > 0) size else 1 << 27

    // 定义hash函数
    def hash(value: String, seed: Int): Long = {
        var result: Long = 0L
        for (i <- 0 until value.length) {
            result = result * seed + value.charAt(i)
        }
        result & (cap - 1)
    }
}

class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount5, String, TimeWindow] {
    // 定义redis连接

    lazy val jedis = new Jedis("localhost", 6379)
    lazy val bloom = new Bloom(1 << 29)

    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount5]): Unit = {
        // 位图的存储方式，key是 windowend ，value是bitmap
        val storeKey = context.window.getEnd.toString

        var count = 0L

        // 把每个窗口的uv count值也存入redis表，存放内存为（windowEnd - > uvCount)，所以要先从redis中读取
        if (jedis.hget("count", storeKey) != null) {
            count = jedis.hget("count", storeKey).toLong
        }

        // 用布隆过滤器判断当前用哪个户是否已经存在
        val userId = elements.last._2.toString
        val offset = bloom.hash(userId, 61)
        // 定义一个标志位，判断redis位图中有没有一位
        val isExist = jedis.getbit(storeKey, offset)
        if (!isExist) {
            // 如果不存在，位图对应位置为1，count +1
            jedis.setbit(storeKey, offset, true)
            jedis.hset("count", storeKey, (count + 1).toString)
            out.collect(UvCount5(storeKey.toLong,count+1))
        }else{
            out.collect(UvCount5(storeKey.toLong,count))
        }
    }
}