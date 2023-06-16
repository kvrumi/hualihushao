package com.atguigu.flink.datastream.transform;

import com.atguigu.flink.func.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SplitStream {
    // 使用侧流来分流
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.addSource(new ClickSource());

        OutputTag<Event> zhangsTag = new OutputTag<>("zhangs", Types.POJO(Event.class));
        OutputTag<Event> lisiTag = new OutputTag<>("lisi", Types.POJO(Event.class));
        OutputTag<Event> tomTag = new OutputTag<>("tom", Types.POJO(Event.class));

        SingleOutputStreamOperator<String> mainDs = ds.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, String>.Context context, Collector<String> collector) throws Exception {
                if ("zhangs".equals(event.getUser())) {
                    // 放入zhangs流
                    context.output(zhangsTag, event);
                } else if ("lisi".equals(event.getUser())) {
                    // 放入lisi流
                    context.output(lisiTag, event);
                } else if ("tom".equals(event.getUser())) {
                    // 放入tom流
                    context.output(tomTag, event);
                } else {
                    // 放入主流
                    collector.collect(event.toString());
                }
            }
        });

        mainDs.print("ceLiu");

        // 通过主流获取侧流
        mainDs.getSideOutput(zhangsTag).print("zhangs");
        mainDs.getSideOutput(lisiTag).print("lisi");
        mainDs.getSideOutput(tomTag).print("tom");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
