package org.czlan.flinklearning._09_sql;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description 从kafka input_kafka 主题消费数据，并生成Table，然后过滤出状态为Success的数据，再写回kafka
 * @date 2021/2/27 18:22
 */
public class Demo04 {
    public static void main(String[] args) throws Exception {
        // todo 0。env
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        // todo 1。source
        TableResult inputTable = bsTableEnv.executeSql(
                "CREATE TABLE input_kafka（\n" +
                        " user_id bigint, \n" +
                        " page_id bigint,\n" +
                        " status String\n" +
                        " ) with (\n" +
                        " 'connector' = 'kafka',\n" +
                        " 'topic' = 'input_kafka',\n" +
                        " 'properties.bootstrap.servers' = 'node1:9092',\n" +
                        " 'properties.group.id' = 'testGroup',\n" +
                        " 'scan.startup.mode' = 'latest-offset',\n" +
                        " 'format' = 'json'\n" +
                        " )"
        );


        // todo 2。transformation
        // 编写sql 过滤出状态为success的数据
        String sql ="select * from input_kafka where status=‘success’";
        Table etlResult = bsTableEnv.sqlQuery(sql);

        // todo 3。sink
        DataStream<Tuple2<Boolean, Row>> resultDS = bsTableEnv.toRetractStream(etlResult, Row.class);
        resultDS.print();

        TableResult outputTable = bsTableEnv.executeSql(
                "CREATE TABLE output_kafka（\n" +
                        " user_id bigint, \n" +
                        " page_id bigint,\n" +
                        " status String\n" +
                        " ) with (\n" +
                        " 'connector' = 'kafka',\n" +
                        " 'topic' = 'input_kafka',\n" +
                        " 'properties.bootstrap.servers' = 'node1:9092',\n" +
                        " 'format' = 'json'\n" +
                        " 'sink.partitioner' = 'round-robin" +
                        " )"
        );

        bsTableEnv.executeSql("insert into output_kafka select * from " + etlResult);


        // todo 4。execute
        bsEnv.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long createTime;
    }
}
