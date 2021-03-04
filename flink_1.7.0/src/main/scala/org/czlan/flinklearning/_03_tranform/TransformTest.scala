package org.czlan.flinklearning._03_tranform

import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.czlan.flinklearning._02_source.SensorReading

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/24 16:23
 * @Description:
 * @Version: V1.0.0.0
 */
object TransformTest {
    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val streamFromFile = env.readTextFile("/Users/chenzhuanglan/OneDrive/learning/WorkSpace/Flink_Learning/src/main/resources/sensor.txt")

        // 1. 基本转换算子和简单聚合算子
        val dataStream: DataStream[SensorReading] = streamFromFile
                .map(
                    data => {
                        val dataArray = data.split(",")
                        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
                    }
                )

        //  val dataStream2: KeyedStream[SensorReading, Tuple] = dataStream.keyBy(0)
        //  val dataStream2: KeyedStream[SensorReading, Tuple] = dataStream.keyBy("id")
        //  val dataStream2: KeyedStream[SensorReading, String] = dataStream.keyBy(_.id)
        val dataStream2 = dataStream.keyBy(_.id)

                // .reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10)).print()
                .sum(2)
        dataStream2.print()


        // 2. 多流转换算子
        // split 和 select
        val splitStream = dataStream.split(data => {
            if (data.temperature > 30) Seq("high") else Seq("low")
        })
        val high = splitStream.select("high")
        val low = splitStream.select("low")
        val all = splitStream.select("high","low")

        high.print("high")
        low.print("low")
        all.print("all")

        // 3. 合并两条流
        // connected
        val warnning = high.map(data => (data.id, data.temperature))

        val connectedStream = warnning.connect(low)

        val coMapDataStream = connectedStream.map(
            warnningData => (warnningData._1,warnningData._2,"warning"),
            lowData => (lowData.id,"healthy")
        )

        val unionStream = high.union(low).union(all)
        unionStream.filter(new FilterFunction[SensorReading] {
            override def filter(value: SensorReading): Boolean = {
                value.id.startsWith("sensor_1")
            }
        })


        env.execute("transform test")
    }

}

class MyFileter extends FilterFunction[SensorReading]{
    override def filter(value: SensorReading): Boolean = {
        value.id.startsWith("sensor_1")
    }
}

class MyMapper extends RichMapFunction[SensorReading,String]{

    override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

    override def open(parameters: Configuration): Unit = super.open(parameters)

    override def close(): Unit = super.close()

    override def map(in: SensorReading): String = {
        "flink"
    }
}