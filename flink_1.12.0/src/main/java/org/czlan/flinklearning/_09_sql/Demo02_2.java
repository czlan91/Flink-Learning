package org.czlan.flinklearning._09_sql;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description 使用table和sql两种方式做wordcount
 * @date 2021/2/27 18:22
 */
public class Demo02_2 {
    public static void main(String[] args) throws Exception {
        // todo 0。env
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        // todo 1。source
        DataStream<WC> wordsDS = bsEnv.fromElements(
                new WC("Hello", 1),
                new WC("word", 1),
                new WC("Hello", 1)
        );


        // todo 2。transformation
        // 将dataStream 转换为 view 和 table
        Table table = bsTableEnv.fromDataStream(wordsDS, $("word"), $("frequency"));

        Table resultTable = table
                .groupBy($("word"))
                .select($("word"), $("frequency").sum().as("frequency"))
                .filter($("frequency").isEqual(2));


        DataStream<Tuple2<Boolean, WC>> resultDS = bsTableEnv.toRetractStream(resultTable, WC.class);


        Table resultTable2 = table
                .groupBy($("word"))
                .select($("word"), $("frequency").sum().as("frequency"));

        DataStream<Tuple2<Boolean, Row>> resultDS2 = bsTableEnv.toRetractStream(resultTable2, Row.class);
        // todo 3。sink
        resultDS.print("WC.class");
        resultDS2.print("Row.class");

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
