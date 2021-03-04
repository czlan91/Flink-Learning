package org.czlan.flinklearning._10_action;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * @author chenzhuanglan
 * @program flink_1.12.0
 * @description 1. 实时计算出当天零点截止到当前时间的销售总额
 * 2。 计算出各个分类的销售top3
 * 3。 每秒更新一次统计结果
 * @date 2021/2/27 22:16
 */
public class DoubleElevenBigScreem {
    public static void main(String[] args) throws Exception {
        // todo 1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // todo 2. source
        DataStream<Tuple2<String, Double>> orderDS = env.addSource(new MySource());

        // todo 3. transformation -- 预聚合 每隔1秒聚合一下各分类的销售总金额

        DataStream<CategoryPojo> tempAggResult = orderDS.keyBy(t -> t.f0)
                // 每隔一天计算一次
                // .window(TumblingProcessingTimeWindows.of(Time.days(1)))
                // 每隔1s计算最近有i天的数据，但是11月11日 00：00：00运算的是 11月10日 00：01：00-11月11日 00：01：00 --不对
                // .window(SlidingProcessingTimeWindows.of(Time.days(1),Time.seconds(1)))

                // 3.1 定义大小为一天的窗口，第二个参数表示中国，使用UTC+08:00时区比UTC时间早
                // 表示从当前的00：00：00开始计算当前的数据，缺少一个触发时机/触发间隔
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                // 3.2 定义一个1s的触发器
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                // 3.3 聚合结果
                // 支持复杂的自定义聚合
                .aggregate(new PriceAggregate(), new WindowResult());
        // 3.4 看一下聚合的结果
        // CategoryPojo(category=男装，totalPrice=17225.36,dataTime=2020-10-20 08:04:12

        tempAggResult.print();

        // todo 4. sink --使用上面预聚合的结果，实现业务需求
        tempAggResult.keyBy(CategoryPojo::getDateTime)
                // 每隔1s进行最后聚合运算
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new FinalResultWindowProcess());


        // todo 5。execute
        env.execute();
    }

    /**
     * 自定义数据源实时产生订单数据 Tuple2<分类，金额>
     */
    public static class MySource implements SourceFunction<Tuple2<String, Double>> {
        private boolean flag = true;
        private String[] categorys = {"女装", "男装", "图书", "家电", "洗护", "美妆", "运动", "游戏", "户外", "家具", "乐器", "办公"};
        private Random random = new Random();

        @Override
        public void run(SourceContext<Tuple2<String, Double>> ctx) throws Exception {
            while (flag) {
                // 随机生成分类和金额
                int index = random.nextInt(categorys.length);
                String category = categorys[index];
                double price = random.nextDouble() * 100;
                ctx.collect(Tuple2.of(category, price));
                Thread.sleep(20);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }


    private static class PriceAggregate implements AggregateFunction<Tuple2<String, Double>, Double, Double> {
        // 初始化累加器
        @Override
        public Double createAccumulator() {
            return 0D;
        }

        // 把数据累加到累加器上
        @Override
        public Double add(Tuple2<String, Double> value, Double accumulator) {
            return value.f1 + accumulator;
        }

        // 获取累加结果
        @Override
        public Double getResult(Double accumulator) {
            return accumulator;
        }


        //合并各个subtask的结果
        @Override
        public Double merge(Double a, Double b) {
            return a + b;
        }
    }


    private static class WindowResult implements WindowFunction<Double, CategoryPojo, String, TimeWindow> {
        private FastDateFormat df = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

        @Override
        public void apply(String category, TimeWindow window, java.lang.Iterable<Double> input, Collector<CategoryPojo> out) throws Exception {
            long currentTimeMillis = System.currentTimeMillis();
            String dateTime = df.format(currentTimeMillis);

            Double totalPrice = input.iterator().next();
            out.collect(new CategoryPojo(category, totalPrice, dateTime));
        }
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class CategoryPojo {
        private String category;
        private double totalPrice;
        private String dateTime;
    }

    private static class FinalResultWindowProcess extends ProcessWindowFunction<CategoryPojo, Object, String, TimeWindow> {

        // key 表示当前这1s的时间
        // elements，截止到当前这1s的数据
        @Override
        public void process(String key, Context context, Iterable<CategoryPojo> elements, Collector<Object> out) throws Exception {
            double total = 0D;
            // 1. 实时计算出当天零点截止到当前时间的销售总额


            // 2。 计算出各个分类的销售top3

            // 小顶堆
            PriorityQueue<CategoryPojo> queue = new PriorityQueue<>(3,
                    (c1, c2) -> c1.getTotalPrice() >= c2.getTotalPrice() ? 1 : -1);
            for (CategoryPojo element :
                    elements) {
                double price = element.getTotalPrice();
                total += price;

                if (queue.size() < 3) {
                    queue.add(element);
                } else {
                    if (price >= queue.peek().getTotalPrice()) {
                        // queue.remove(queue.peek());
                        queue.poll(); // 移除堆顶元素
                        queue.add(element); // 或offer入队
                    }
                }
            }

            List<String> top3List = queue.stream().sorted((c1, c2) -> c1.getTotalPrice() >= c2.getTotalPrice() ? -1 : 1)
                    .map(c -> "分类：" + c.getTotalPrice() + ",金额：" + c.getTotalPrice())
                    .collect(Collectors.toList());

            // 3。 每秒更新一次统计结果
            System.out.println("时间：" + System.currentTimeMillis() + "总金额："+ total);
            System.out.println("top3： \n"+ StringUtils.join(top3List,"\n"));
        }
    }
}
