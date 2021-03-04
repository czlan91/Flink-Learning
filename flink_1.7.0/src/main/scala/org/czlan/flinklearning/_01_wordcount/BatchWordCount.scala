package org.czlan.flinklearning._01_wordcount

import org.apache.flink.api.scala._

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/23 14:59
 * @Description: 批处理代码
 * @Version: V1.0.0.0
 */
object BatchWordCount {
    def main(args: Array[String]): Unit = {
        // 创建一个批处理的执行环境
        val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

        // 从文件中读取数据
        val inputPath = "/Users/chenzhuanglan/OneDrive/learning/WorkSpace/Flink_Learning/src/main/resources/hello.txt"

        val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

        // 分词之后做count
        val wordCountDataSet = inputDataSet.flatMap(_.split(" "))
                // 元组，下标从0开始
                .map((_, 1))
                .groupBy(0)
                .sum(1)

        // 打印输出
        wordCountDataSet.print()
        env.execute("batch word count job")


    }
}