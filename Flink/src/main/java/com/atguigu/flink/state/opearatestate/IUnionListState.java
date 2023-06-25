package com.atguigu.flink.state.opearatestate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class IUnionListState {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.enableCheckpointing(2000);

        env.socketTextStream("hadoop102", 8888)
                .map(new MyMapFunction())
                .addSink(new SinkFunction<String>() {
                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        if (value.contains("x")) {
                            //人为手动抛出一个异常，为了模拟故障的情况
                            throw new RuntimeException("出异常了!");
                        }
                        System.out.println(value);
                    }
                });


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static class MyMapFunction implements MapFunction<String, String>, CheckpointedFunction {

        ListState<String> strs;

        @Override
        public String map(String value) throws Exception {
            strs.add(value);
            return strs.get().toString();
        }

        /*
            周期性(默认200ms，可以调整)将状态以快照的形式进行备份
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //备份是自动进行的，无需进行任何手动操作
        }

        /*
            初始化： 在第一次启动或任务失败重启后执行，从之前的备份中找到状态，重新赋值。

            FunctionInitializationContext context: 程序的运行环境，或上下文，可以从中获取很多额外的信息。
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 找到备份状态的存储设备
            OperatorStateStore operatorStateStore = context.getOperatorStateStore();
            // 从备份中找到之前备份的变量
            ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("state", String.class);

            strs = operatorStateStore.getUnionListState(listStateDescriptor);
        }
    }

}
