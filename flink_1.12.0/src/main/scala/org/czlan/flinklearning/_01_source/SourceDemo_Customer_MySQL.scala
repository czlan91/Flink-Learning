package org.czlan.flinklearning._01_source

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
 * @program flink-1.12.0
 * @description ${description}
 * @author chenzhuanglan
 * @date 2021/2/19 16:22
 * @version ${version}
 */
object SourceDemo_Customer_MySQL {
  def main(args: Array[String]): Unit = {
    // todo 0.env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)

    // todo 1.source
    // 如果MySQLSource后面需要添加参数的话，就是缺少 隐式转换，添加 import org.apache.flink.streaming.api.scala._
    val studentDS: DataStream[Student] = env.addSource(new MySQLSource()).setParallelism(1)

    // todo 2.transformation


    // todo 3.sink
    studentDS.print()

    // todo 4.execute
    env.execute()
  }
}

case class Student(id: Int, name: String, age: Int)

class MySQLSource extends RichParallelSourceFunction[Student] {

  var flag: Boolean = true
  var conn: Connection = _
  var ps: PreparedStatement = _
  var rs: ResultSet = _

  /**
   * 连接 mysql
   *
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?useSSL=false", "root", "123456")
    val sql = "select id,name,age from t_student"
    ps = conn.prepareStatement(sql)
  }

  override def close(): Unit = {
    if (ps != null) ps.close()
    if (conn != null) conn.close()

  }

  override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
    while (flag) {
      rs = ps.executeQuery()
      while (rs.next()){
        val id = rs.getInt("id")
        val name = rs.getString("name")
        val age = rs.getInt("age")
        ctx.collect(Student(id,name,age))
      }
      Thread.sleep(5000)
    }

  }

  override def cancel(): Unit = {
    flag = false
  }
}