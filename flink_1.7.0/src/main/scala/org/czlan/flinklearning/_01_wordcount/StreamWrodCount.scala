package org.czlan.flinklearning._01_wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/23 15:45
 * @Description: 流处理代码
 * @Version: V1.0.0.0
 */
object StreamWrodCount {

    def helper(): Unit = {
        val str =
            """
              | usage: xxxx.jar --host localhost --port 7777
              |""".stripMargin
        print(str)
    }

    def main(args: Array[String]): Unit = {

        try {
            val params = ParameterTool.fromArgs(args)
            val host: String = params.get("host")
            val port: Int = params.getInt("port")


            print(f"host:${host},port:${port}")

            // 创建一个流处理的执行环境
            val env = StreamExecutionEnvironment.getExecutionEnvironment


//            env.setParallelism(1)


            env.disableOperatorChaining()

//            env.setRuntimeMode(RuntimeExecutionMode.BATCH)

            // 接收socket数据流
            val textDataStream = env.socketTextStream(host, port)

            // 逐一读取数据，打散之后进行wordcount
            val wrodCountDataStream = textDataStream.flatMap(_.split("\\s"))
                    .filter(_.nonEmpty).disableChaining()
                    .startNewChain()
                    .map((_, 1))
                    .keyBy(0)
                    .sum(1)

            // 打印输出
            wrodCountDataStream.print()
                    // 设置并行度
                    .setParallelism(1)

            // 启动流式处理程序
            env.execute("stream word count job")
        } catch {
            case e: Exception =>
                e.printStackTrace()
                helper()

        }
    }
}