package org.czlan.flinklearning._10_jobs

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/26 11:31
 * @Description:  窗口预聚合，然后根据不同的窗口进行输出
 * @Version: V1.0.0.0
 */
// 定义输入数据的样例类
case class UserBehavior(UserId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 定义窗口聚合结果的样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object AggregateTest {
    def main(args: Array[String]): Unit = {
        // 1. 创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


        // 2. 读取数据
        val dataStream = env.readTextFile("/Users/chenzhuanglan/WorkSpace/Flink_Learning/Flink_Learning/flink_1.7.0/src/main/resources/UserBehavior.csv")
                .map(data => {
                    val dataArray = data.split(",")
                    UserBehavior3(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
                })
                .assignAscendingTimestamps(_.timestamp * 1000L)

        // 3. transform 处理数据
        val processedStream = dataStream
                .filter(_.behavior == "pv")
                .keyBy(_.itemId)
                .timeWindow(Time.hours(1), Time.minutes(5))
                // 窗口聚合
//                .aggregate(new CountAgg(), new WindowResult())
                .aggregate(new CountAgg(), new WindowResult())
                // 按照窗口分组
                .keyBy(_.windowEnd)
                .process(new TopNHotItems(3))



        // 4. sink 控制台输出
        processedStream.print()

        env.execute("hot item job")

    }

}

// 自定义预聚合函数， 输出类型 就是 WindowResult的输入类型
class CountAgg() extends AggregateFunction[UserBehavior3, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior3, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

// 自定义预聚合函数计算平均数
class AverageAgg() extends AggregateFunction[UserBehavior3, (Long, Int), Double] {
    override def createAccumulator(): (Long, Int) = (0, 0)

    override def add(value: UserBehavior3, accumulator: (Long, Int)): (Long, Int) = (accumulator._1 + value.timestamp, accumulator._2 + 1)

    override def getResult(accumulator: (Long, Int)): Double = accumulator._2 / accumulator._1

    override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = (a._1 + b._1, a._2 + b._2)
}

// 自定义窗口函数，输出ItemViewCount
class WindowResult extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
    override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
        out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
    }
}

class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

    private var itemState: ListState[ItemViewCount] = _


    override def open(parameters: Configuration): Unit = {
        itemState = getRuntimeContext.getListState[ItemViewCount](new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
    }

    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
        // 把每条数据存入状态列表
        itemState.add(value)
        // 注册定时器
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    // 定时器触发时，对所有数据排序，并输出
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
        // 将所有state中的数据取出，放到一个List Buffer中
        val allItems: ListBuffer[ItemViewCount] = new ListBuffer[ItemViewCount]
        import scala.collection.JavaConversions._
        for (item <- itemState.get()) {
            allItems += item
        }
        // 按照count大小排序,并取前N个
        val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

        // 清空状态
        itemState.clear()

        // 将排名结果格式化输出
        val result: StringBuilder = new StringBuilder
        result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

        // 输出每一个商品的信息
        for (i <- sortedItems.indices) {
            val currentItem = sortedItems(i)
            result.append("No").append(i + 1).append(":")
                    .append(" 商品ID=").append(currentItem.itemId)
                    .append(" 浏览量=").append(currentItem.count)
                    .append("\n")
        }

        result.append("=====================")

        // 控制输出评率
        Thread.sleep(1000)

        out.collect(result.toString())
    }


}