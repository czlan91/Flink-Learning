package org.czlan.flinklearning._05_window;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description 基于会话窗口
 * @date 2021/2/21 18:29
 */
public class WindowDemo_5 {
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

            // 需求1：设置会话超时时间为10s，10s内没有数据到来，则触发上一个窗口的计算（前提是上一个窗口得有数据）
            KeyedStream<CarInfo, String> keyedDS = carDS.keyBy(CarInfo::getSensorId);

            SingleOutputStreamOperator<CarInfo> result = keyedDS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                    .sum("count");

            // todo 需要深入研究下 ProcessingTimeSessionWindows 和 DynamicProcessingTimeSessionWindows
            // https://cloud.tencent.com/developer/article/1381251
            // https://cloud.tencent.com/developer/article/1539537


            // todo 3.sink
            result.print();


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
