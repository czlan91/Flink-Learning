package org.czlan.flinklearning._10_jobs

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/27 08:53
 * @Description: 订单超时检测
 * @Version: V1.0.0.0
 */

// 定义输入订单事件的样例类
case class OrderEvent(orderid: Long, eventType: String, txId: String, eventTime: Long)

// 定义输出结果样例类
case class OrderResult(OrderId: Long, resultMsg: String)

object OrderTimeout {
    def main(args: Array[String]): Unit = {
        // 1. 创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 用相对路径
        val resource = getClass.getResource("/OrderLog.csv")

        // 2. 读取数据
        val dataStream = env.readTextFile(resource.getPath)
                .map(data => {
                    val dataArray = data.split(",")
                    OrderEvent12(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
                })
                .assignAscendingTimestamps(_.eventTime * 1000)
                .keyBy(_.orderid)

        // 3. 定义匹配模式
        val orderPayPattern = Pattern.begin[OrderEvent12]("begin").where(_.eventType == "create")
                .followedBy("follow").where(_.eventType == "pay")
                .within(Time.minutes(15))


        // 在事件流上应用模式，得到一个pattern stream
        val patternStream = CEP.pattern(dataStream,orderPayPattern)

        // 从patternStream 上应用select function ，检出匹配到的序列,超时的事件要报警处理
        val orderTimeoutOutputTag = new OutputTag[OrderResult12]("orderTimeout")
        val orderPayDataStream = patternStream.select(orderTimeoutOutputTag,new OrderTimeoutSelect(),new OrderPaySelect())



        // 4. sink 控制台输出
        orderPayDataStream.print("payed")
        orderPayDataStream.getSideOutput(orderTimeoutOutputTag).print("timeout:")

        env.execute("order timeout job")
    }



}
// 自定义超时事件序列处理函数
class OrderTimeoutSelect()  extends PatternTimeoutFunction[OrderEvent12,OrderResult12]{
    override def timeout(map: util.Map[String, util.List[OrderEvent12]], timeoutTimestamp: Long): OrderResult12 = {
        val timeoutOrderId = map.get("begin").iterator().next().orderid
        OrderResult12(timeoutOrderId,"timedout")
    }
}

// 自定义正常支付事件序列处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent12,OrderResult12]{
    override def select(map: util.Map[String, util.List[OrderEvent12]]): OrderResult12 = {
        val payOrderId = map.get("follow").iterator().next().orderid
        OrderResult12(payOrderId,"payed success")
    }
}