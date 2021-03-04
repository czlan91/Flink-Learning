package org.czlan.flinklearning._10_jobs

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.runtime.aggregate.KeyedProcessFunctionWithCleanupState
import org.apache.flink.util.Collector
import org.czlan.flinklearning._10_jobs.UniqueVisitor.getClass

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/26 17:06
 * @Description:
 * @Version: V1.0.0.0
 */
// 输入的广告点击事件样例类
case class AdClickEvent(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

// 按照省份统计的输出结果样例类
case class CountByProvince(windowEnd: String, province: String, count: Long)

// 输出的黑名单报警信息
case class BlackListWarining(userId: Long, adId: Long, msg: String)

class AdStatisticsByGeo {
    // 定义侧输出流的tag
    val blackListOutputTag: OutputTag[BlackListWarining] = new OutputTag[BlackListWarining]("blackList")

    def main(args: Array[String]): Unit = {
        // 1. 创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


        // 2. 读取数据
        val resource = getClass.getResource("/UserBehavior.csv")
        val dataStream = env.readTextFile(resource.getPath)
                .map(data => {
                    val dataArray = data.split(",")
                    AdClickEvent(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim, dataArray(4).trim.toLong)
                })
                .assignAscendingTimestamps(_.timestamp * 1000L)

        // 自定义 process function, 过滤大量刷点击的行为
        val filterBlackListStream = dataStream
                .keyBy(data => (data.userId, data.adId))
                .process(new FilterBlackListUser(100))


        // 3. transform 处理数据
        val processedStream = filterBlackListStream
                .keyBy(_.province)
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new AdCountAgg(), new AdcountResult())


        // 4. sink 控制台输出
        processedStream.print()

        filterBlackListStream.getSideOutput(blackListOutputTag).print("blackList")

        env.execute("adclick job")

    }

    class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {
        // 定义状态，保存当前用户对当前广告的点击量
        lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))
        // 保存是否发送过黑名单的状态
        lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("issent-state", classOf[Boolean]))
        // 保存定时器触发的时间戳
        lazy val resetTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state", classOf[Long]))

        override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
            // 取出count的state
            val curCount = countState.value()

            // 如果是第一次处理，注册定时器,每天00:00触发
            if (curCount == 0) {
                val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
                resetTimer.update(ts)
                ctx.timerService().registerProcessingTimeTimer(ts)
            }

            // 判断计算是否达到上限，如果达到则加入黑名单
            if(curCount >= maxCount){
                // 判断是否发送过黑名单，只发送一次
                if(!isSentBlackList.value()){
                    isSentBlackList.update(true)
                    // 输出 到侧输出流
                    ctx.output(blackListOutputTag,BlackListWarining(value.userId,value.adId,"Click over"+maxCount+"times today."))
                }
                return
            }

            // 计数状态加1，输出数据到主流
            countState.update(curCount+1)
            out.collect(value)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
            // 定时器触发时，清空状态
            if(timestamp == resetTimer.value()){
                isSentBlackList.clear()
                countState.clear()
                resetTimer.clear()
            }
        }
    }

}

// 自定义预聚合函数
class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

class AdcountResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {

        out.collect(CountByProvince(new Timestamp(window.getEnd).toString, key, input.iterator.next()))
    }
}
