package org.czlan.flinklearning._10_jobs

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/26 14:27
 * @Description:   乱序数据，在标记数据时不允许延迟，但是在开窗时，运行延迟；窗口预聚合
 * @Version: V1.0.0.0
 */
// 输入数据样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

// 窗口聚合结果样例类
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlow {
    def main(args: Array[String]): Unit = {


        // 1. 创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


        // 2. 读取数据
        val dataStream = env.readTextFile("/Users/chenzhuanglan/WorkSpace/Flink_Learning/Flink_Learning/flink_1.7.0/src/main/resources/apachetest.log")
                .map(data => {
                    val dataArray = data.split(" ")
                    val simpleDataFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                    val timestamp = simpleDataFormat.parse(dataArray(3).trim).getTime
                    ApacheLogEvent(dataArray(0).trim, dataArray(1).trim, timestamp, dataArray(5).trim, dataArray(6).trim)
                })
                // 虽然有乱序数据，
                .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
                    override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
                })
                .keyBy(_.url)
                .timeWindow(Time.minutes(10),Time.seconds(5))
                // 关闭窗口的时候，允许的延迟时间。
                .allowedLateness(Time.seconds(60))
                .aggregate(new CountAgg2() , new WindowResult2())
                        .keyBy(_.windowEnd)
                        .process(new TopNHotUrls(5))




        // 4. sink 控制台输出
        dataStream.print()

        env.execute("network flow job")

    }
}
// 自定义预聚合函数， 输出类型 就是 WindowResult的输入类型
class CountAgg2() extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数，输出ItemViewCount
class WindowResult2 extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
        out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
    }
}

// 自定义排序输出处理函数
class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

    lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState[UrlViewCount](new ListStateDescriptor[UrlViewCount]("item-state", classOf[UrlViewCount]))

    override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
        // 把每条数据存入状态列表
        urlState.add(value)
        // 注册定时器
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    // 定时器触发时，对所有数据排序，并输出
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
        // 将所有state中的数据取出，放到一个List Buffer中
        val allUrlViews: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]
        val iter = urlState.get().iterator()
        while (iter.hasNext){
            allUrlViews += iter.next()
        }

        // 清空状态
        urlState.clear()


        val sortedUrlViews = allUrlViews.sortWith(_.count > _.count).take(topSize)

        // 将排名结果格式化输出
        val result: StringBuilder = new StringBuilder
        result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

        // 输出每一个商品的信息
        for (i <- sortedUrlViews.indices) {
            val currentUrlViews = sortedUrlViews(i)
            result.append("No").append(i + 1).append(":")
                    .append(" URL=").append(currentUrlViews.url)
                    .append(" 浏览量=").append(currentUrlViews.count)
                    .append("\n")
        }

        result.append("=====================")

        // 控制输出评率
        Thread.sleep(1000)

        out.collect(result.toString())
    }

}