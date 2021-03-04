package org.czlan.flinklearning._09_sql;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description 使用table和sql两种方式做wordcount
 * @date 2021/2/27 18:22
 */
public class Demo02 {
    public static void main(String[] args) throws Exception {
        // todo 0。env
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        // todo 1。source
        DataStream<WC> wordsDS = bsEnv.fromElements(
                new WC("Hello", 1),
                new WC("sord", 1),
                new WC("Hello", 1)
        );


        // todo 2。transformation
        // 将dataStream 转换为 view 和 table
        bsTableEnv.createTemporaryView("t_words",wordsDS,$("word"),$("frequency"));

        String sql = "select word,sum(frequency) as frequency " +
                " from t_words " +
                " group by word";

        Table resultTable = bsTableEnv.sqlQuery(sql);

        // 转换为DataStream
        DataStream<Tuple2<Boolean, WC>> resultDS = bsTableEnv.toRetractStream(resultTable, WC.class);

        /*
        7> (true,Demo02.WC(word=Hello, frequency=1))
        7> (true,Demo02.WC(word=sord, frequency=1))
        7> (false,Demo02.WC(word=Hello, frequency=1))   先删除原始数据，
        7> (true,Demo02.WC(word=Hello, frequency=2))    更新一条新数据
         */

        // todo 3。sink
        resultDS.print();

        // todo 4。execute
        bsEnv.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class WC {
        private String word;
        private Integer frequency;
    }
}
