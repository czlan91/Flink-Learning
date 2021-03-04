package org.czlan.flinklearning._10_jobs

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/27 10:43
 * @Description:
 * @Version: V1.0.0.0
 */
//定义接收流事件的样例类
case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)

object TxMacthDetect {
    val unmatchedPays = new OutputTag[OrderEvent12]("unmatchedPays")
    val unmatchReceipts = new OutputTag[ReceiptEvent14]("unmatchReceipts")

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
                .filter(_.txId != "")
                .assignAscendingTimestamps(_.eventTime * 1000)
                .keyBy(_.txId)

        // 支付到账事件流
        val receiptResource = getClass.getResource("/ReceiptLog.csv")
        val receiptStream = env.readTextFile(receiptResource.getPath)
                .map(data => {
                    val dataArray = data.split(",")
                    ReceiptEvent14(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim.toLong)
                })
                .assignAscendingTimestamps(_.eventTime * 1000)
                .keyBy(_.txId)

        // 将两条流连接起来，共同处理
        val processedStream = dataStream.connect(receiptStream)
                .process(new TxPayMatch())



        // 4. sink 控制台输出
        processedStream.print("tx")
        processedStream.getSideOutput(unmatchReceipts).print("unmatchReceipts")
        processedStream.getSideOutput(unmatchedPays).print("unmatchedPays")

        env.execute("tx job")
    }

    class TxPayMatch() extends CoProcessFunction[OrderEvent12, ReceiptEvent14, (OrderEvent12, ReceiptEvent14)] {
        // 定义状态来保存已经达到的订单支付事件和到账事件
        lazy val payState: ValueState[OrderEvent12] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent12]("pay-state", classOf[OrderEvent12]))
        lazy val receiptState: ValueState[ReceiptEvent14] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent14]("pay-state", classOf[ReceiptEvent14]))

        // 订单支付事件数据的处理
        override def processElement1(pay: OrderEvent12, ctx: CoProcessFunction[OrderEvent12, ReceiptEvent14, (OrderEvent12, ReceiptEvent14)]#Context, out: Collector[(OrderEvent12, ReceiptEvent14)]): Unit = {
            // 判断有没有对应的到账事件
            val receipt = receiptState.value()
            if (receipt != null) {
                // 如果已经有receipt，在主流输出匹配信息
                out.collect((pay, receipt))
                receiptState.clear()
            } else {
                // 如果还没到，那么把pay存入状态，并且注册一个定时器等待
                payState.update(pay)
                ctx.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 5000L)
            }
        }

        // 到账事件的处理
        override def processElement2(receipt: ReceiptEvent14, ctx: CoProcessFunction[OrderEvent12, ReceiptEvent14, (OrderEvent12, ReceiptEvent14)]#Context, out: Collector[(OrderEvent12, ReceiptEvent14)]): Unit = {
            // 判断有没有对应的到账事件
            val pay = payState.value()
            if (pay != null) {
                // 如果已经有receipt，在主流输出匹配信息
                out.collect((pay, receipt))
                payState.clear()
            } else {
                // 如果还没到，那么把pay存入状态，并且注册一个定时器等待
                receiptState.update(receipt)
                ctx.timerService().registerEventTimeTimer(receipt.eventTime * 1000L + 5000L)
            }
        }

        override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent12, ReceiptEvent14, (OrderEvent12, ReceiptEvent14)]#OnTimerContext, out: Collector[(OrderEvent12, ReceiptEvent14)]): Unit = {
            // 到时间了，如果还没有收到某个事件，那么输出报警信息
            if(payState.value()!= null){
                // recipt没来，输出pay到侧输出流
                ctx.output(unmatchedPays,payState.value())
            }
            if (receiptState.value() != null){
                ctx.output(unmatchReceipts,receiptState.value())
            }
            payState.clear()
            receiptState.clear()
        }
    }

}

