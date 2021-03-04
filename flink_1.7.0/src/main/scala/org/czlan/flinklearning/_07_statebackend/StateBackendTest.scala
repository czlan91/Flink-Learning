package org.czlan.flinklearning._07_statebackend

import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
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

            env.enableCheckpointing(6000)

            //            env.setStateBackend( new RocksDBStateBackend())

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

            val processedStream2 = dataStream.keyBy(_.id)
                    //                            .process(new TempChangeAlert(10.0))
                    .flatMap(new MyFlatMapFunction(10.0))

            val processedStream3 = dataStream.keyBy(_.id)
                    //                            .process(new TempChangeAlert(10.0))
                    .flatMapWithState[(String, Double, Double), Double] {
                        // 如果没有状态的话，也就是没有数据来过，那么就将当前数据温度存入状态
                        case (input: SensorReading, None) => (List.empty, Some(input.temperature))
                        // 如果有状态，就应该与上次的温度值比较差值，如果大于阈值就输出报警
                        case (input: SensorReading, lastTemp: Some[Double]) =>
                            val diff = (input.temperature - lastTemp.get).abs
                            if (diff > 10.0) {
                                (List((input.id, lastTemp.get, input.temperature)), Some(input.temperature))
                            } else {
                                (List.empty, Some(input.temperature))
                            }
                    }



            dataStream.print("input:")
            processedStream.print("process:")
            processedStream2.print("process2:")
            processedStream3.print("process3:")

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

class TempChangeAlert(threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {

    // 定义一个状态变量，保存上次的温度值
    lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lasttemp", classOf[Double]))


    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {
        // 获取上次的温度值
        val lastTemp = lastTempState.value()

        // 用当前的温度值和上次的求差，如果大于阈值，输出告警信息
        val diff = (value.temperature - lastTemp).abs

        if (diff > threshold) {
            out.collect((value.id, lastTemp, value.temperature))
        }
        lastTempState.update(value.temperature)

    }

}


class MyFlatMapFunction(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

    // 定义一个状态变量，保存上次的温度值
    //    lazy val lastTempState:ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lasttemp",classOf[Double]))

    private var lastTempState: ValueState[Double] = _


    override def open(parameters: Configuration): Unit = {
        // 初始化的时候声明state变量
        lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lasttemp", classOf[Double]))
    }

    override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
        // 获取上次的温度值
        val lastTemp = lastTempState.value()

        // 用当前的温度值和上次的求差，如果大于阈值，输出告警信息
        val diff = (value.temperature - lastTemp).abs

        if (diff > threshold) {
            out.collect((value.id, lastTemp, value.temperature))
        }
        lastTempState.update(value.temperature)

    }

}