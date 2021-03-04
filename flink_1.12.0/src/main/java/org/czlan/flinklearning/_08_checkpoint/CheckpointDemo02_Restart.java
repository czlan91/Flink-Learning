package org.czlan.flinklearning._08_checkpoint;

import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description 重启策略
 * @date 2021/2/27 15:07
 */
public class CheckpointDemo02_Restart {
    public static void main(String[] args) throws Exception {
        // todo 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // checkpoint 参数设置
        // 类型1： 必须参数
        // 设置checkpoint的时间间隔为1000ms做一次checkpoint，其实就是每隔1000ms发一次Barrier。
        env.enableCheckpointing(1000);

        // 设置State状态存储介质
        // Memory：state存内存，checkpoint存内存，-- 开发不用
        // FS：state存内存，checkpoint存fs（本地\hdfs） -- 一般情况下使用
        // RocksDB：state存RocksDB（内存+磁盘），checkpoint存fs（本地/hdfs）---超大状态使用，但是对于状态的读写效率要低一点。
        /*if (args.length>0){
            env.setStateBackend(new FsStateBackend(args[0]));
        }else{
            env.setStateBackend(new FsStateBackend("file:///d:\\data\\ckp"));
        }*/

        if (SystemUtils.IS_OS_WINDOWS) {
            env.setStateBackend(new FsStateBackend("file:///d:\\data\\ckp"));
        } else {
            env.setStateBackend(new FsStateBackend("hdfs://node1:8020/flink-checkpint/checkpoint"));
        }
        // 类型2：建议参数
        // 设置两个checkpoint 之间最少等待时间，如果设置checkpoint之间最少是要等500ms（为了避免每隔1000ms做一次checkpoint的时候，前一次太慢和后一次重叠
        // 一起去了）
        // 如：高速公路，每隔1s关口放行一辆车，但是规定了两车之间的最小车距为500m
        // 默认为 0
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 设置如果在做checkpoint过程中出现错误，是否让整体任务失败，true是，false不是,默认是true
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        // 默认为值0，表示不容忍任何检查点失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        // 设置是否清理检查点，表示cancel时是否需要保留当前的checkpoint，默认checkpoint会在作业被cancel时被删除
        // CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION : true,当作业被取消时，删除外部的checkpoint（默认值）
        // CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION : false,当作业被取消时，保留外部的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        // 类型3：直接shying默认的即可
        // 设置 checkpoint的执行模式为 exactly_once(默认）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置 checkpoint的超时时间  ，如果  checkpoint在60s内尚未完成说明该次  checkpoint失败，则丢弃,默认10分钟
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 设置同一时间有多少个checkpoint可以同时执行,默认为1
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);


        // todo 配置重启策略
        // 1。 配置了checkpoint的情况下不做任何配置，默认是无限重启并自动修复，可以解决小问题，但是可能会隐藏真正的BUG
        // 2。 单独配置无重启厕率
        env.setRestartStrategy(RestartStrategies.noRestart());
        // 3.  固定延迟重启 ,最多重启3次，每次重启时间间隔   --- 开发中常用
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));
        // 4。 失败率重启,  -- 偶尔使用
        // 5分钟内job失败不超过3次，自动重启，每次间隔10s，如果5分钟内程序失败达到3次，则程序退出
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                // 每个测量阶段内最大失败次数
                3,
                // 失败率测量的时间间隔
                Time.of(5, TimeUnit.MINUTES),
                // 两次连续重启的时间间隔
                Time.of(10, TimeUnit.SECONDS)));


        // todo 2。source
        DataStreamSource<String> linesDS = env.socketTextStream("localhost", 9999);


        // todo 3. transformation
        // 3.1 切割处每隔单词并记录为1
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = linesDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word :
                        words) {

                    if (word.equals("bug")) {
                        System.out.println("bug....");
                        throw new Exception("bug....");
                    }
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        // 3.2 分组
        // 批处理是 groupby ，流处理的分组是keyby
        KeyedStream<Tuple2<String, Integer>, String> groupedDS = wordAndOneDS.keyBy(t -> t.f0);
        // 3.3 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> aggResult = groupedDS.sum(1);

        SingleOutputStreamOperator<String> result = aggResult.map(new RichMapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> value) throws Exception {
                return value.f0 + ":::" + value.f1;
            }
        });

        // todo 4. sink
        result.print();

        Properties props = new Properties();
        props.setProperty("boostrap.servers", "localhost:9092");

        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>("flink kafka", new SimpleStringSchema(), props);
        result.addSink(kafkaSink);


        // 5. execute
        env.execute();
    }
}
