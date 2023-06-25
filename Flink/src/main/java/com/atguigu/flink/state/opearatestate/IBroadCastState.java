package com.atguigu.flink.state.opearatestate;

import com.atguigu.flink.func.WaterSensorMapFunction;
import com.atguigu.flink.pojo.WaterSensor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class IBroadCastState {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.enableCheckpointing(2000);
        // 1.准备两个流
        SingleOutputStreamOperator<WaterSensor> dataDs = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        SingleOutputStreamOperator<MyConf> confDs = env.socketTextStream("hadoop102", 9999)
                .map(
                        line -> {
                            String[] strings = line.split(",");
                            return new MyConf(strings[0].trim(), strings[1].trim());
                        }
                );
        // 2.普通流无法使用广播状态，必须是广播流
        MapStateDescriptor<String, MyConf> mapStateDescriptor = new MapStateDescriptor<>("config", String.class, MyConf.class);
        BroadcastStream<MyConf> broadcastStream = confDs.broadcast(mapStateDescriptor);

        // 3.让两个流产生联系
        BroadcastConnectedStream<WaterSensor, MyConf> connectedStream = dataDs.connect(broadcastStream);

        connectedStream.process(
                // 处理数据流中的数据
                new BroadcastProcessFunction<WaterSensor, MyConf, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, MyConf, WaterSensor>.ReadOnlyContext ctx, Collector<WaterSensor> out) throws Exception {
                        // 获取广播状态，取出最新的配置
                        ReadOnlyBroadcastState<String, MyConf> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        MyConf myConf = broadcastState.get(value.getId());
                        // 使用配置
                        value.setId(myConf.getName());
                        out.collect(value);
                    }

                    @Override
                    public void processBroadcastElement(MyConf value, BroadcastProcessFunction<WaterSensor, MyConf, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                        // 把新的配置，放入广播状态即可
                        BroadcastState<String, MyConf> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        // 把广播状态当Map用
                        broadcastState.put(value.getId(), value);
                        // 自动广播到数据流的每一个Task
                    }
                }
        );

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MyConf {
        private String id;
        private String name;
    }

}
