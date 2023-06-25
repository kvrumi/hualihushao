package com.atguigu.flink.state.keyedstate;

import com.atguigu.flink.func.WaterSensorMapFunction;
import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Created by Smexy on 2023/6/20
 * <p>
 * 输出每种传感器不重复的水位值
 * 去重:
 * Set：  flink没有提供Set类型的状态。
 * Map<key,value>：使用。把希望去重的数据作为key放入Map
 */
public class IMapState {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        ds.keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {

                            private MapState<Integer, Object> mapState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                mapState = getRuntimeContext().getMapState(
                                        new MapStateDescriptor<>("map", Integer.class, Object.class)
                                );
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                mapState.put(value.getVc(), "");
                                out.collect(ctx.getCurrentKey() + "当前不重复的VC" + mapState.keys().toString());
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
