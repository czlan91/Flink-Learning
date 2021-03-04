package org.czlan.flinklearning._05_window

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.czlan.flinklearning._02_source.SensorReading

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/25 09:53
 * @Description:
 * @Version: V1.0.0.0
 */
object WindowTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)


        val streamFromFile = env.readTextFile("/Users/chenzhuanglan/OneDrive/learning/WorkSpace/Flink_Learning/src/main/resources/sensor.txt")
        // 从调用时刻开始给env创建的每一个stream追加时间特征
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        // 设置 自动的生成的 时间
//        env.getConfig.setAutoWatermarkInterval(100L)

        // 开启增量更新
        val backend = new RocksDBStateBackend("hdfs://namenode:40010/flink/checkpoints", true)
        env.setStateBackend(backend.asInstanceOf[StateBackend])
        env.enableCheckpointing(5000)
        env.getCheckpointConfig.setCheckpointInterval(500)

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,500))
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,org.apache.flink.api.common.time.Time.seconds(300),org.apache.flink.api.common.time.Time.seconds(5)))

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

        // 统计10秒内的最小温度
        val minTempPerWindowStream = dataStream
                .map(
                    data => (data.id, data.temperature)
                )
                .keyBy(_._1)
                // 设置时区 滑动窗口
//                .window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5),Time.hours(-8)))
                .timeWindow(Time.seconds(10)) // 时间窗口
                .reduce((data1, data2) => (data1._1, data1._2.min(data2._2))) // 用reduce 做增量聚合


        minTempPerWindowStream.print()

        env.execute("window test")

    }

}

// 周期性
class PeriodicAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
    val bound: Long = 60 * 1000 //  延时1分钟
    var maxTs: Long = Long.MinValue // 观察到的最大时间戳

    override def getCurrentWatermark: Watermark = {
        new Watermark(maxTs - bound)
    }

    override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
        maxTs = maxTs.max(element.timestamp * 1000)
        element.timestamp
    }
}

// 周期性，乱序
class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {
    // 抽取时间戳
    override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
}


// 间断性
class PunctuateAssigner extends AssignerWithPunctuatedWatermarks[SensorReading] {
    val bound: Long = 60 * 1000 //  延时1分钟

    override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
        if (lastElement.id == "sensor_1") {
            new Watermark(extractedTimestamp - bound)
        } else {
            null
        }
    }

    override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = element.timestamp
}

class MyProcess() extends KeyedProcessFunction[String,SensorReading,String]{
    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
        ctx.timerService().registerEventTimeTimer(2000)
    }
}