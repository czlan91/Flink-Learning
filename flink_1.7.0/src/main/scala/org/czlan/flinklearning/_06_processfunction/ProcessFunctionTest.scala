package org.czlan.flinklearning._06_processfunction

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.czlan.flinklearning._02_source.SensorReading
import org.czlan.flinklearning._05_window.PeriodicAssigner

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/25 11:20
 * @Description:
 * @Version: V1.0.0.0
 */
object ProcessFunctionTest {
    def main(args: Array[String]): Unit = {
        def main(args: Array[String]): Unit = {
            val env = StreamExecutionEnvironment.getExecutionEnvironment

            env.setParallelism(1)


            val streamFromFile = env.readTextFile("/Users/chenzhuanglan/OneDrive/learning/WorkSpace/Flink_Learning/src/main/resources/sensor.txt")
            // 从调用时刻开始给env创建的每一个stream追加时间特征
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
            // 设置 自动的生成的 时间
            //        env.getConfig.setAutoWatermarkInterval(100L)


            // 转换为String 方便输出
            val dataStream: DataStream[SensorReading] = streamFromFile
                    .map(
                        data => {
                            val dataArray = data.split(",")
                            SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
                        }
                    )
                    // 指定时间戳和watermark 升序数据，准时发车，单位是毫秒
                    //                .assignAscendingTimestamps(_.timestamp * 1000)
                    .assignTimestampsAndWatermarks(new PeriodicAssigner())

            val processedStream = dataStream
                    .keyBy(_.id)
                    .process(new TempIncreAlert)

            dataStream.print("input:")
            processedStream.print("process:")

            env.execute("KeyedProcessFunction test")

        }
    }
}


class TempIncreAlert() extends KeyedProcessFunction[String, SensorReading, String] {

    // 定义一个状态，用来保存上一个数据的温度值
    lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

    // 定义一个状态，用来保存定时器的时间错
    lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer", classOf[Long]))

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
        // 先取出上一个温度值
        val preTemp = lastTemp.value()
        // 更新温度值
        lastTemp.update(value.temperature)

        val cuTimerTs = currentTimer.value()

        // 温度上升了且没有设置过定时器，则注册定时器
        if (value.temperature > preTemp && cuTimerTs == 0) {
            val timerTs = ctx.timerService().currentProcessingTime() + 1000L
            ctx.timerService().registerProcessingTimeTimer(timerTs)
            currentTimer.update(timerTs)
        } else if (value.temperature < preTemp || preTemp == 0.0) {
            // 如果温度下降或是第一条数据，删除定时器并清除状态
            ctx.timerService().deleteProcessingTimeTimer(cuTimerTs)
            currentTimer.clear()
        }
    }

    // 定时器任务
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
        super.onTimer(timestamp, ctx, out)
        // 输出告警信息
        out.collect(ctx.getCurrentKey + "温度连续上升")
        currentTimer.clear()
    }

}