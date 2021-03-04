package org.czlan.flinklearning._05_window;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description 基于数量的滚动和滑动窗口
 * @date 2021/2/21 18:29
 */
public class WindowDemo_3_4 {
        public static void main(String[] args) throws Exception {
            // todo 0.env
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

            // todo 1.source
            DataStream<String> lines = env.socketTextStream("localhost",9999);


            // todo 2.transformation
            SingleOutputStreamOperator<CarInfo> carDS = lines.map(new MapFunction<String, CarInfo>() {
                @Override
                public CarInfo map(String value) throws Exception {
                    String[] arr = value.split(" ");
                    return new CarInfo(arr[0], Integer.parseInt(arr[1]));
                }
            });

            // 需求1：分组后的数据，统计在最近的5条消息中，各自路口通过的汽车数量，相同的key，每出现5次进行统计-- 基于数量的滑动窗口
            SingleOutputStreamOperator<CarInfo> result1 = carDS.keyBy(CarInfo::getSensorId)
                    // .timeWindow(Time.seconds(5))
                    .countWindow(5)
                    .sum("count");

            // 需求2：分组后的数据，统计在最近的5条消息中，各自路口通过的汽车数量，相同的key，每出现3次进行统计-- 基于数量的滑动窗口
            SingleOutputStreamOperator<CarInfo> result2 = carDS.keyBy(CarInfo::getSensorId)
                    .countWindow(5,3)
                    .sum("count");

            // todo 3.sink
            result1.print();
            result2.print();


            // todo 4.execute
            env.execute();
        }

        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        @Builder
        public static class CarInfo{
            private String sensorId; // 信号灯id
            private Integer count; // 通过该信号灯的车的数量
        }
}
