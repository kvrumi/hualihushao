package com.atguigu.flink.timeAndwindow;


import com.atguigu.flink.func.ClickSource;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.pojo.OrderDetailEvent;
import com.atguigu.flink.pojo.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 窗口联结
 * 能进入到同一个窗口的数据才有资格进行join ，如果来自两条流的数据的key一样，就可以join到一起。
 */
public class IWindowJoin {

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
        orderDs.join(orderDetailDs)
                .where(OrderEvent::getOrderId)
                .equalTo(OrderDetailEvent::getOrderId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<OrderEvent, OrderDetailEvent, String>() {
                    /**
                     * 处理join成功的数据
                     */
                    @Override
                    public String join(OrderEvent first, OrderDetailEvent second) throws Exception {
                        return first + " -- " + second ;
                    }
                })
                .print("join");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
