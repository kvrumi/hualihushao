package com.atguigu.flink.timeAndwindow;


import com.atguigu.flink.pojo.OrderDetailEvent;
import com.atguigu.flink.pojo.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 间隔联结 IntervalJoin
 * 以一条流为主，设置一个时间间隔(以当前数据的时间 ，设置一个上界和下界) ,另外一条流中相同key的数据如果落到了间隔的范围内，就可以join成功。
 */
public class IIntervalJoin {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // OrderDetailEvent
        // detail-1,order-1,Apple,3000
        SingleOutputStreamOperator<OrderEvent> orderDs = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] split = line.split(",");
                            return new OrderEvent(split[0].trim(), Long.valueOf(split[1].trim()));
                        }
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                );

        SingleOutputStreamOperator<OrderDetailEvent> orderDetailDs = env.socketTextStream("hadoop102", 9999)
                .map(
                        line -> {
                            String[] split = line.split(",");
                            return new OrderDetailEvent(split[0].trim(), split[1].trim(), split[2].trim(), Long.valueOf(split[3].trim()));
                        }
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderDetailEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                );

        // order流 与 detail流的join
        orderDs.keyBy(OrderEvent::getOrderId)
                .intervalJoin(orderDetailDs.keyBy(OrderDetailEvent::getOrderId))
                .between(Time.seconds(-2),Time.seconds(2))
                .process(
                        new ProcessJoinFunction<OrderEvent, OrderDetailEvent, String>() {
                            @Override
                            public void processElement(OrderEvent left, OrderDetailEvent right, ProcessJoinFunction<OrderEvent, OrderDetailEvent, String>.Context ctx, Collector<String> out) throws Exception {
                                out.collect(left + " -- " + right );
                            }
                        }
                )
                .print("join");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
