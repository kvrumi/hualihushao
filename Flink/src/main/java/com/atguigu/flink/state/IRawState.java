package com.atguigu.flink.state;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Smexy on 2023/6/20
 *
 *  flink中计算可以分为有状态的计算和无状态的计算。
 *      无状态计算： ETL
 *                  每一条数据在计算时，相对独立，不会使用人和其他数据留下的信息。
 *      有状态计算:  wordcount
 *                  每一条数据需要和上一条数据留下的信息，继续运算。
 *
 *      状态： 在Task中保留的供后续数据计算使用的变量(数据)，称为状态。
 *
 *  -------------------------------------------------
 *      分类:
 *          RawState： 原始状态。客户自己定义和维护的状态。
 *                          维护： 定期备份状态，尝试在Task失败时，恢复状态。
 *
 *          ManagedState: 管理状态。使用Flink提供的状态功能定义的状态。Flink自动维护状态。
 */

public class IRawState {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.enableCheckpointing(2000);

        DataStreamSink<String> ds = env.socketTextStream("hadoop102", 8888)
                .map(new MyMapFunction())
                .addSink(
                        new SinkFunction<String>() {
                            @Override
                            public void invoke(String value, Context context) throws Exception {
                                if (value.contains("x")) {
                                    throw new RuntimeException();
                                }
                                System.out.println("value = " + value);
                            }
                        }
                );


        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    public static class MyMapFunction implements MapFunction<String,String>{

        List<String> strings = new ArrayList<>();

        @Override
        public String map(String  value) throws Exception {
            strings.add(value);
            return strings.toString();
        }
    }


}


