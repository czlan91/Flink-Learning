package org.czlan.flinklearning._10_jobs

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/26 18:04
 * @Description: 登录失败检测  用cep来实现
 * @Version: V1.0.0.0
 */
// 输入的登录时间样例类
case class LoginEvent10(userId: Long, ip: String, eventType: String, eventTime: Long)

//输出的异常告警信息样例类
case class Warning10(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFailWithCep {
    def main(args: Array[String]): Unit = {
        // 1. 创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 用相对路径
        val resource = getClass.getResource("/LoginLog.csv")

        // 2. 读取数据
        val dataStream = env.readTextFile(resource.getPath)
                .map(data => {
                    val dataArray = data.split(",")
                    LoginEvent10(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent10](Time.seconds(5)) {
                    override def extractTimestamp(element: LoginEvent10): Long = element.eventTime
                })
                .keyBy(_.userId)

        // 3. 定义匹配模式
        val loginFailPattern = Pattern.begin[LoginEvent10]("begin").where(_.eventType == "fail")
                .next("next").where(_.eventType == "fail")
                .within(Time.seconds(2))


        // 在事件流上应用模式，得到一个pattern stream
        val patternStream = CEP.pattern(dataStream,loginFailPattern)

        // 从patternStream 上应用select function ，检出匹配到的序列
        val loginFialDataStream = patternStream.select(new LoginFailMatch())



        // 4. sink 控制台输出
        loginFialDataStream.print()

        env.execute("login detect job")

    }

}

class LoginFailMatch() extends PatternSelectFunction[LoginEvent10, Warning10] {
    override def select(map: util.Map[String, util.List[LoginEvent10]]): Warning10 = {
        // 从map中按照名称取出对应的事件
        val firstFail = map.get("begin").iterator().next()
        val lastFail = map.get("next").iterator().next()

        Warning10(firstFail.userId,firstFail.eventTime,lastFail.eventTime,"login fail!")
    }
}