package org.czlan.flinklearning._03_sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * @program flink-1.12.0
 * @description ${description}
 * @author chenzhuanglan
 * @date 2021/2/19 22:21
 * @version ${version}
 */
object SinkDemo_MySQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val dataStream = env.fromElements(Student(-1, "tom", 18))

    // sink
    dataStream.addSink(new MyJDBCSink())

    env.execute("sink test")
  }

}

case class Student(id: Int, name: String, age: Int)

class MySQLSink extends RichSinkFunction[Student] {
  override def invoke(value: Student, context: SinkFunction.Context): Unit = super.invoke(value, context)

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()
}

class MyJDBCSink extends RichSinkFunction[Student] {
  // 定义sql连接，预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  // 初始化，创建连接和预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "123456")
    insertStmt = conn.prepareStatement("INSERT INTO t_student (id, name, age) VALUES (null , ?, ?);")
    updateStmt = conn.prepareStatement("UPDATE t_student SET name=? and age=? WHERE id=?")
  }

  // 调用连接执行SQL
  override def invoke(value: Student, context: SinkFunction.Context): Unit = {
    // 执行更新语句
    updateStmt.setString(1, value.name)
    updateStmt.setInt(2, value.age)
    updateStmt.execute()
    // 如果update没有查到数据，则执行插入语句
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.name)
      insertStmt.setInt(2, value.age)
      insertStmt.setInt(3, value.id)
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

