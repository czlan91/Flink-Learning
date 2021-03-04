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

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description 将dataStream数据转换为table或者view然后使用sql进行统计查询
 * @date 2021/2/27 18:22
 */
public class Demo01 {
    public static void main(String[] args) throws Exception {
        // todo 0。env
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        // todo 1。source
        DataStream<Order> orderA = bsEnv.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)
        ));

        DataStream<Order> orderB = bsEnv.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "diaper", 3),
                new Order(4L, "rubber", 1)
        ));

        // todo 2。transformation 和 datastram数据进行转换table和view然后查询
        Table tableA = bsTableEnv.fromDataStream(orderA, $("user"), $("product"), $("amount"));
        tableA.printSchema();
        System.out.println(tableA);
        bsTableEnv.createTemporaryView("tableB", orderB, $("user"), $("product"), $("amount"));

        // 查询 tableA中 amount>2 和 tableB中amount>1的数据合并
        /*
        select * from tableA where amount>2
        union
        select * from tableB where amount>1
         */
        String sql = "select * from " + tableA + " where amount>2  \n" +
                " union \n" +
                " select * from tableB where amount>1";
        Table resultTable = bsTableEnv.sqlQuery(sql);
        resultTable.printSchema();
        System.out.println(resultTable);


        // 将表转换为datastream

        // toAppendStream 将计算后的数据append到结果DataStream中去
        // toRetractStream 将计算后的数据在DataStream原数据的基础上更新true或是删除false
        // DataStream<Order> resultDS1 = bsTableEnv.toAppendStream(resultTable, Order.class);
        DataStream<Tuple2<Boolean, Order>> resultDS2 = bsTableEnv.toRetractStream(resultTable, Order.class);

        // todo 3。sink
        // resultDS1.print();
        resultDS2.print();

        // todo 4。execute
        bsEnv.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class Order {
        private Long user;
        private String product;
        private int amount;
    }
}
