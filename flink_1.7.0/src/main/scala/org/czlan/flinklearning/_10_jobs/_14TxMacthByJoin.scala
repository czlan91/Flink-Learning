package org.czlan.flinklearning._10_jobs

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, ProcessJoinFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/27 10:43
 * @Description:
 * @Version: V1.0.0.0
 */
//定义接收流事件的样例类
case class ReceiptEvent14(txId: String, payChannel: String, eventTime: Long)

object TxMacthByJoin {
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
        val processedStream = dataStream.intervalJoin(receiptStream)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new TxPayMatchByJoin())



        // 4. sink 控制台输出
        processedStream.print("tx")
        processedStream.getSideOutput(unmatchReceipts).print("unmatchReceipts")
        processedStream.getSideOutput(unmatchedPays).print("unmatchedPays")

        env.execute("tx match by join job")
    }

    class TxPayMatchByJoin() extends ProcessJoinFunction[OrderEvent12, ReceiptEvent14, (OrderEvent12, ReceiptEvent14)] {
        // 定义状态来保存已经达到的订单支付事件和到账事件
        lazy val payState: ValueState[OrderEvent12] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent12]("pay-state", classOf[OrderEvent12]))
        lazy val receiptState: ValueState[ReceiptEvent14] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent14]("pay-state", classOf[ReceiptEvent14]))

        override def processElement(left: OrderEvent12, right: ReceiptEvent14, ctx: ProcessJoinFunction[OrderEvent12, ReceiptEvent14, (OrderEvent12, ReceiptEvent14)]#Context, out: Collector[(OrderEvent12, ReceiptEvent14)]): Unit = {
            out.collect(left,right)
        }
    }

}

