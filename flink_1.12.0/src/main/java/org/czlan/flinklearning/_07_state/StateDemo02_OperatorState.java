package org.czlan.flinklearning._07_state;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Iterator;

/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description 使用OperatorState中的 ListSate 模拟kafka source 进行offset维护
 * @date 2021/2/22 18:10
 */
public class StateDemo02_OperatorState {
    public static void main(String[] args) throws Exception {
        // todo 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 并行度设置为1，方便观察
        env.setParallelism(1);
        // 先直接使用下面的代码设置checkpoint时间间隔和磁盘路径以及代码遇到异常后的重启策略，
        env.enableCheckpointing(1000);
        env.setStateBackend(new FsStateBackend("file:///Users/chenzhuanglan/WorkSpace/Flink_Learning/Flink_Learning/flink_1.12.0/sql"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 固定延迟重启策略，程序出错异常的时候，重启2次，每次延迟3s重启，超过2次程序退出。
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2,3000));
        // todo 1.source

        DataStreamSource<String> ds = env.addSource(new MyKafkaSource()).setParallelism(1);


        // todo 2.transformation

        // todo 3.sink
        ds.print();

        // todo 4.execute
        env.execute();
    }

    // 使用OperatorState中的 listState 模拟kafkaSource 进行Offset维护
    public static class MyKafkaSource extends RichParallelSourceFunction<String> implements CheckpointedFunction{
        // 1. 声明ListState
        private ListState<Long> offsetState = null;
        private Long offset = 0L;

        private boolean flag = true;


        // 2. 初始化创建liststate
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Long> stateDescriptor = new ListStateDescriptor<>("offsetstate", Long.class);
            offsetState = context.getOperatorStateStore().getListState(stateDescriptor);

        }

        // 3. 使用state
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
           while(flag){
               Thread.sleep(1000);
               Iterator<Long> iterator = offsetState.get().iterator();
               if (iterator.hasNext()){
                   offset = iterator.next();
               }
               offset +=1;

               int subTaskid = getRuntimeContext().getIndexOfThisSubtask();

               if (offset%5 == 0){
                   throw new Exception("bug出现了");
               }
               ctx.collect("subTaskid:"+subTaskid+",当前的offset值为："+offset);
           }
        }

        // 4. state checkpoint
        // 该方法会定时执行，将state状态从内存存入checkpoint中
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            offsetState.clear(); // 清理内存数据并存入checkpoint磁盘中
            offsetState.add(offset);
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
