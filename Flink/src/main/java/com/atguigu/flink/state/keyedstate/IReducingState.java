package com.atguigu.flink.state.keyedstate;

import com.atguigu.flink.func.WaterSensorMapFunction;
import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 *  计算每种传感器的水位和
 */

public class IReducingState {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        ds.keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {

                            private ReducingState<Integer> reducingState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                            }
                        }
                );

    }

}
