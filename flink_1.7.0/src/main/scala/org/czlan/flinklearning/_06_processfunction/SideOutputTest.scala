package org.czlan.flinklearning._06_processfunction

import com.sun.java.accessibility.util.TopLevelWindowMulticaster
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.czlan.flinklearning._02_source.SensorReading
import org.czlan.flinklearning._05_window.PeriodicAssigner

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/25 11:50
 * @Description:
 * @Version: V1.0.0.0
 */
object SideOutputTest {
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
                    .process(new FreezingAlert)

            dataStream.print("input:")
            processedStream.print("process:")

            // 侧输出流
            processedStream.getSideOutput(new OutputTag[String]("freezing alert")).print("alert data")

            env.execute("sideOutput test")

            
        }
    }
}

// 冰点报警，如果小于32F，输出报警信息到侧输出流
// 主输出流的数据类型
class FreezingAlert extends ProcessFunction[SensorReading,SensorReading]{

    lazy val alertOutput:OutputTag[String] = new OutputTag[String]("freezing alert")

    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
        if(value.temperature < 32.0){
            // 将数据保存到侧输出流
            ctx.output(alertOutput,"freezing alert for +" + value.id)
        } else {
            out.collect(value)
        }
    }
}