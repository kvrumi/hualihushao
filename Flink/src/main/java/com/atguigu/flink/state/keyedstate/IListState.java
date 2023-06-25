package com.atguigu.flink.state.keyedstate;

import com.atguigu.flink.func.WaterSensorMapFunction;
import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * 针对每种传感器输出最高的3个水位值。
 */
public class IListState {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000);

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        ds.keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {

                            private ListState<Integer> listState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                listState = getRuntimeContext().getListState(
                                        new ListStateDescriptor<>("list", Integer.class)
                                );
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                listState.add(value.getVc());
                                List<Integer> collect = StreamSupport.stream(listState.get().spliterator(), true)
                                        .sorted(Comparator.reverseOrder())
                                        .limit(3)
                                        .collect(Collectors.toList());
                                listState.update(collect);
                                out.collect(ctx.getCurrentKey() + "前3" + collect);
                            }
                        }
                )
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
