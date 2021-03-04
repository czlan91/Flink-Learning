package org.czlan.flinklearning._10_jobs

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.czlan.flinklearning._10_jobs.OrderTimeout.getClass

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/27 09:26
 * @Description: 使用状态管理，来检测订单超时
 * @Version: V1.0.0.0
 */

// 定义输入订单事件的样例类
case class OrderEvent12(orderid: Long, eventType: String, txId: String, eventTime: Long)

// 定义输出结果样例类
case class OrderResult12(OrderId: Long, resultMsg: String)

object _12OrderTimeoutWIthoutCep {
    val orderTimeoutOutputTag = new OutputTag[OrderResult12]("ordertimeout")

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

        // 定义process function，进行超时检测
        //        val timeoutWarningStream = dataStream.process(new OrderTimeoutWarning())

        val orderResultStream = dataStream.process(new OrderPayMatch())
        // 4. sink 控制台输出
        orderResultStream.print("payed")

        env.execute("order timeout job")
    }

    class OrderPayMatch extends KeyedProcessFunction[Long, OrderEvent12, OrderResult12] {
        // 保存pay是否来过的状态
        lazy val ispayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))

        // 保存定时器的时间戳
        lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time-state", classOf[Long]))

        //

        override def processElement(value: OrderEvent12, ctx: KeyedProcessFunction[Long, OrderEvent12, OrderResult12]#Context, out: Collector[OrderResult12]): Unit = {
            // 先读取状态
            val isPayed = ispayedState.value()
            val timerTs = timerState.value()

            // 根据时间的类型进行分类判断，做不同的处理逻辑
            if (value.eventType == "create") {
                // 如果是create 事件，接下来判断pay是否来过
                if (isPayed) {
                    // 如果已经pay过，匹配成功，输出主流，清空状态
                    out.collect(OrderResult12(value.orderid, "payed successfully"))
                    ctx.timerService().deleteEventTimeTimer(timerTs)
                    ispayedState.clear()
                    timerState.clear()
                } else {
                    // 如果没有pay过，注册定时器等待pay的到来
                    val ts = value.eventTime * 1000L + 15 * 60 * 1000L
                    ctx.timerService().registerEventTimeTimer(ts)
                    timerState.update(ts)
                }
            } else if (value.eventType == "pay") {

                // 如果是pay事件，那么判断是否create过，用timer表示
                if (timerTs > 0) {
                    // 如果有定时器，表示已经有create来过
                    // 继续判断，是否超过timeout时间
                    if (timerTs > value.eventTime * 1000L) {
                        // 如果定时器时间还没到，那么输出成功匹配
                        out.collect(OrderResult12(value.orderid, "payed successfully"))
                    } else {
                        // 如果当前pay的时间已经超时，那么输出到侧输出流
                        ctx.output(orderTimeoutOutputTag, OrderResult12(value.orderid, "payed  timeout"))
                    }
                    ctx.timerService().deleteEventTimeTimer(timerTs)
                    ispayedState.clear()
                    timerState.clear()
                } else {
                    // pay 先到了，更新状态，注册定时器等待create
                    ispayedState.update(true)
                    ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L)
                }
            }
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent12, OrderResult12]#OnTimerContext, out: Collector[OrderResult12]): Unit = {
            // 根据状态的值，判断哪个数据的值没有来
            if (ispayedState.value()){
                // 如果为true,表示pay先到，没等到create
                ctx.output(orderTimeoutOutputTag, OrderResult12(ctx.getCurrentKey, "already payed but not found create log"))
            }else{
                // 表示create先到，没等到pay
                ctx.output(orderTimeoutOutputTag, OrderResult12(ctx.getCurrentKey, "payed timeout"))
            }
            ispayedState.clear()
            timerState.clear()
        }
    }

}

class OrderTimeoutWarning extends KeyedProcessFunction[Long, OrderEvent12, OrderResult12] {
    // 保存pay是否来过的状态
    lazy val ispayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))

    override def processElement(value: OrderEvent12, ctx: KeyedProcessFunction[Long, OrderEvent12, OrderResult12]#Context, out: Collector[OrderResult12]): Unit = {
        // 先取出状态标志位
        val isPayed = ispayedState.value()

        if (value.eventType == "create" && !isPayed) {
            // 如果遇到了create事件，并且pay没有来过，注册定时器开始等待
            ctx.timerService().registerEventTimeTimer(value.eventTime * 1000 + 15 * 60 * 1000L)
        } else if (value.eventType == "pay") {
            // 如果是pay事件，直接把状态改为true
            ispayedState.update(true)
        }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent12, OrderResult12]#OnTimerContext, out: Collector[OrderResult12]): Unit = {
        // 判断 ispayed 是否为true
        val isPayed = ispayedState.value()
        if (isPayed) {
            out.collect(OrderResult12(ctx.getCurrentKey, "order payed successfully"))
        } else {
            out.collect(OrderResult12(ctx.getCurrentKey, "order payed timeout"))
        }
    }
}