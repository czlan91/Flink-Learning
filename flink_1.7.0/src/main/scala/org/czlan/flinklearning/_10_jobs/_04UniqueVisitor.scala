package org.czlan.flinklearning._10_jobs

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/26 15:14
 * @Description:  global window 自定义函数，
 * @Version: V1.0.0.0
 */
// 定义输入数据的样例类
case class UserBehavior4(UserId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//
case class UvCount(windowEnd:Long,UvCount:Long)

object UniqueVisitor {
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
                .timeWindowAll(Time.hours(1))
                .apply(new UvCountByWindow())


        // 4. sink 控制台输出
        processedStream.print()

        env.execute("uv job")

    }

}

class UvCountByWindow() extends AllWindowFunction[UserBehavior3,UvCount5,TimeWindow]{
    override def apply(window: TimeWindow, input: Iterable[UserBehavior3], out: Collector[UvCount5]): Unit = {
        // 定义一个 scala set，用于保存所有的数据userId并去重
        var idSet = Set[Long]()
        // 把当前窗口所有数据的ID收集到set中，最后输出set的大小
        for ( userBehavior <- input){
            idSet += userBehavior.UserId
        }

        out.collect(UvCount5(window.getEnd,idSet.size))
    }
}