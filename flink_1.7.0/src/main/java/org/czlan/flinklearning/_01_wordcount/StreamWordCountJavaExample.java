package org.czlan.flinklearning._01_wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author：chenzhuanglan
 * @Email：czlan91@live.cn
 * @Date: 2020/1/23 18:27
 * @Description:
 * @Version:
 */
public class StreamWordCountJavaExample {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        // 获取 批处理的
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataStreamSource<String> text;
        if (params.has("host") && params.has("port")) {
            // read the text file from given input path
            text = env.socketTextStream(params.get("host"), params.getInt("port"));
        } else {
            // get default test text data
            System.out.println("Executing WordCount example with default host and port set.");
            System.out.println("Use --host and --port to set host and port socket");
            text = env.socketTextStream("localhost", 7777);
        }

        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(0)
                        .sum(1);

        // emit result
        if (params.has("output")) {
            // 写入到文件，文件路径，行分隔符，列分隔符
            counts.writeAsCsv(params.get("output"), FileSystem.WriteMode.NO_OVERWRITE,"\n", " ");
            // execute program
            env.execute("stream WordCount Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }

    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

}
