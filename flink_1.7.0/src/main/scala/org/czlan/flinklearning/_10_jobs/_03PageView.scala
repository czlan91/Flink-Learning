package org.czlan.flinklearning._10_jobs

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/26 14:54
 * @Description:  pv操作 开窗后 wordcount
 * @Version: V1.0.0.0
 */

// 定义输入数据的样例类
case class UserBehavior3(UserId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

object PageView {
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
                        .map(data => ("pv",1))
                        .keyBy(_._1)
                        .timeWindow(Time.hours(1))
                        .sum(1)


        // 4. sink 控制台输出
        processedStream.print()

        env.execute("pv job")

    }

}