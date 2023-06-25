package com.atguigu.flink.state.keyedstate;

import com.atguigu.flink.func.WaterSensorMapFunction;
import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 检测每种传感器的水位值，如果连续的两个水位值超过10，就输出报警
 */
public class IValueState {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(2000);

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedDS = ds.keyBy(WaterSensor::getId);

        // 使用KeyedStream之后在处理时，使用状态，全部都是 KeyedState
        keyedDS.process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {

                            private ValueState<Integer> valueState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("last", Integer.class));
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                Integer lastVcNum = valueState.value();
                                if (lastVcNum != null && lastVcNum > 10 && value.getVc() > 10) {
                                    out.collect("连续的两个水位值超过10");
                                } else {
                                    out.collect(value.toString());
                                }
                                valueState.update(value.getVc());
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
