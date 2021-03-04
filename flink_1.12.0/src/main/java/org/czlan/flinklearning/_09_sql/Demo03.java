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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description 使用事件时间+watermaker+window 完成订单统计
 * @date 2021/2/27 18:22
 */
public class Demo03 {
    public static void main(String[] args) throws Exception {
        // todo 0。env
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        // todo 1。source
        DataStreamSource<Order> orderDS = bsEnv.addSource(new RichSourceFunction<Order>() {
            private Boolean isRunning = true;

            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random = new Random();
                while (isRunning) {
                    Order order = new Order(UUID.randomUUID().toString(), random.nextInt(3), random.nextInt(101), System.currentTimeMillis());
                    TimeUnit.SECONDS.sleep(1);
                    ctx.collect(order);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });


        // todo 2。transformation
        // 将dataStream 转换为 view 和 table
        SingleOutputStreamOperator<Order> orderDSWithWatermark = orderDS.assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((order, timestamp) -> order.getCreateTime()));


         // 转换为View，注意：指定列的时候，需要手动指定那一列是时间
        bsTableEnv.createTemporaryView("t_order",orderDSWithWatermark,$("orderId"),$("userId"),$("money"),$("createTime").rowtime());

        String sql = "select userId,count(orderId) as orderCount,max(money) as maxMoneym,min(money) as minMoney " +
                " from t_order " +
                " group by userId , tumble(createTime,INTERVAL '5' SECOND)";

        Table resultTable = bsTableEnv.sqlQuery(sql);

        DataStream<Tuple2<Boolean, Row>> resultDS = bsTableEnv.toRetractStream(resultTable, Row.class);

        // todo 3。sink
        resultDS.print();

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
