package org.czlan.flinklearning._10_jobs

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/26 16:21
 * @Description:
 * @Version: V1.0.0.0
 */
// 输入数据样例类
case class MarketingUserBehavior(userId: String, behavior: String, channnel: String, timestamp: Long)

// 输出结果样例类
case class MarketingViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

object AppMarketingByChannel {
    def main(args: Array[String]): Unit = {
        // 1. 创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


        // 2. 读取数据
        val dataStrem = env.addSource(new SimulatedEventSource7())
                .assignAscendingTimestamps(_.timestamp)
                .filter(_.behavior != "UNINSTALL")
                .map(data => {
                    ((data.channnel,data.behavior),1L)
                })
                .keyBy(_._1)  // 以渠道和类型做为key分组
                .timeWindow(Time.hours(1),Time.seconds(10))
                .process(new MarketingCountByChannel())

        dataStrem.print()
        env.execute("MarketingUserBehavior job")
    }
}

// 自定义数据源
class SimulatedEventSource extends RichSourceFunction[MarketingUserBehavior7] {
    // 定义是否运行的标志位
    var running = true

    // 定义用户行为的集合
    val behaviorTypes: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
    // 定义渠道集合
    val channelSets: Seq[String] = Seq("wechat", "weibo", "appstore", "huaweistore")

    // 定义一个随机数发生器
    val rand: Random = new Random()

    override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior7]): Unit = {
        // 定义一个生成数据的上限
        val maxElements = Long.MaxValue
        var count = 0L

        // 随机生成所有数据
        while (running && count < maxElements) {
            val id = UUID.randomUUID().toString
            val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
            val channel = channelSets(rand.nextInt(channelSets.size))
            val ts = System.currentTimeMillis()
            ctx.collect(MarketingUserBehavior7(id, behavior, channel, ts))
        }
    }

    override def cancel(): Unit = {
        running = false
    }
}

// 自定义处理函数
class MarketingCountByChannel() extends ProcessWindowFunction[((String,String),Long),MarketingViewCount7,(String,String),TimeWindow]{
    override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount7]): Unit = {
        val startTs = new Timestamp(context.window.getStart).toString
        val endTs = new Timestamp(context.window.getEnd).toString

        val channel = key._1
        val behavior = key._2

        val count = elements.size
        out.collect(MarketingViewCount7(startTs,endTs,channel,behavior,count))
    }
}
