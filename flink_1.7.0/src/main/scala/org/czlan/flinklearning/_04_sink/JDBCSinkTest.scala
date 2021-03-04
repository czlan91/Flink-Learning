package org.czlan.flinklearning._04_sink

import java.sql.{Connection, Driver, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.czlan.flinklearning._02_source.SensorReading

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/24 19:14
 * @Description:
 * @Version: V1.0.0.0
 */
object JDBCSinkTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)


        val streamFromFile = env.readTextFile("/Users/chenzhuanglan/OneDrive/learning/WorkSpace/Flink_Learning/src/main/resources/sensor.txt")

        // 转换为String 方便输出
        val dataStream: DataStream[SensorReading] = streamFromFile
                .map(
                    data => {
                        val dataArray = data.split(",")
                        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
                    }
                )

        // sink

        dataStream.addSink(new MyJDBCSink())


        env.execute("kafka sink test")
    }

}

class MyJDBCSink() extends RichSinkFunction[SensorReading]{
    // 定义sql连接，预编译器
    var conn:Connection = _
    var insertStmt:PreparedStatement = _
    var updateStmt:PreparedStatement = _

    // 初始化，创建连接和预编译语句
    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","123456")
        insertStmt = conn.prepareStatement("INSERT INTO temperatures (sensor,temp) VALUES (?,?)")
        updateStmt = conn.prepareStatement("UPDATE temperatures SET temp=? WHERE sensor=?")
    }

    // 调用连接执行SQL
     override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
        // 执行更新语句
        updateStmt.setDouble(1,value.temperature)
        updateStmt.setString(2,value.id)
        updateStmt.execute()
        // 如果update没有查到数据，则执行插入语句
        if (updateStmt.getUpdateCount == 0){
            insertStmt.setString(1,value.id)
            insertStmt.setDouble(2,value.temperature)
            insertStmt.execute()
        }

    }

    // 关闭连接
    override def close(): Unit = {
        super.close()
        insertStmt.close()
        updateStmt.close()
        conn.close()
    }
}
