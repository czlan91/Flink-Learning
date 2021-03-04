package org.czlan.flinklearning._10_jobs

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/26 18:04
 * @Description: 登录失败检测
 * @Version: V1.0.0.0
 */
// 输入的登录时间样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

//输出的异常告警信息样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFail {
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

        // 3. transform 处理数据
        val warningStream = dataStream
                .keyBy(_.userId)
                .process(new LoginWarning(2))


        // 4. sink 控制台输出
        warningStream.print()

        env.execute("login detect job")

    }

}

class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent10, Warning10] {
    // 定义状态，保存2秒内的所有登录失败事件
    lazy val loginFailState: ListState[LoginEvent10] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent10]("login-fail-state", classOf[LoginEvent10]))

    override def processElement(value: LoginEvent10, ctx: KeyedProcessFunction[Long, LoginEvent10, Warning10]#Context, out: Collector[Warning10]): Unit = {
//        val loginFailList = loginFailState.get()
//        // 判断类型是否是fail，只添加fail的事件到状态
//        if (value.eventType == "fail") {
//
//            if (!loginFailList.iterator().hasNext) {
//                ctx.timerService().registerEventTimeTimer(value.eventTime * 1000 + 2000)
//            }
//            loginFailState.add(value)
//
//        } else {
//            // 如果是成功，清空状态
//            loginFailState.clear()
//        }

        if(value.eventType == "fail") {
            // 如果是失败，判断之前是否有登录失败事件
            val iter = loginFailState.get().iterator()
            if (iter.hasNext){
                // 如果已经有登录失败事件，就比较事件时间
                val firstFail = iter.next()
                if(value.eventTime < firstFail.eventTime + 2){
                    // 如果两次间隔小于2秒，就输出报警
                    out.collect(Warning10(value.userId,firstFail.eventTime,value.eventTime,"login fail in 2 seconds."))
                }
                // 更新最近一次的登录失败事件，保存在状态里
                loginFailState.clear()
                loginFailState.add(value)
            }else{
                // 如果是第一次登录失败，直接添加到状态
                loginFailState.add(value)
            }
        }else{
            // 如果是成功，清空状态
                        loginFailState.clear()
        }


    }

//    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
//        // 触发定时器的时候，根据状态里面的失败个数决定是否输出报警
//        val allLoginFails: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]
//        val iter = loginFailState.get.iterator()
//        while (iter.hasNext) {
//            allLoginFails += iter.next()
//        }
//
//        // 判断个数
//        if(allLoginFails.length >= maxFailTimes){
//            out.collect(Warning(allLoginFails.head.userId,allLoginFails.head.eventTime,allLoginFails.last.eventTime,"login fail in 2 seconds for "+ allLoginFails.length))
//        }
//
//        loginFailState.clear()
//    }
}