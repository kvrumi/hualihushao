package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

public class GroupWindowTableApi {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new WaterSensor(fields[0].trim(), Long.valueOf(fields[1].trim()), Integer.valueOf(fields[2].trim()));
                        }
                );

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.STRING())
                .column("vc", DataTypes.INT())
                .column("ts", DataTypes.BIGINT())
                .columnByExpression("pt", "PROCTIME")
                .columnByExpression("et", "TO_TIMESTAMP_LTZ(ts,3)")
                .watermark("et", "et - INTERVAL '1' SECOND")
                .build();

        Table table = tableEnvironment.fromDataStream(ds, schema);

        table.printSchema();

        // 分组窗口
        // 滚动
        // 计数滚动窗口
        TumbleWithSizeOnTimeWithAlias w1 = Tumble.over(rowInterval(5L)).on($("pt")).as("w");
        // 处理时间滚动窗口
        TumbleWithSizeOnTimeWithAlias w2 = Tumble.over(lit(5).seconds()).on($("pt")).as("w");
        // 事件时间滚动窗口
        TumbleWithSizeOnTimeWithAlias w3 = Tumble.over(lit(5).seconds()).on($("et")).as("w");

        // 滑动
        // 计数滑动窗口
        SlideWithSizeAndSlideOnTimeWithAlias w4 =
                Slide.over(rowInterval(5L)).every(rowInterval(3L)).on($("pt")).as("w");
        // 处理时间滑动窗口
        SlideWithSizeAndSlideOnTimeWithAlias w5 =
                Slide.over(lit(5).seconds()).every(lit(3).seconds()).on($("pt")).as("w");
        // 事件时间滑动窗口
        SlideWithSizeAndSlideOnTimeWithAlias w6 =
                Slide.over(lit(10).seconds()).every(lit(5).seconds()).on($("et")).as("w");

        // 会话
        // 处理时间会话窗口
        SessionWithGapOnTimeWithAlias w7 = Session.withGap(lit(5).seconds()).on($("pt")).as("w");
        // 事件时间会话窗口
        SessionWithGapOnTimeWithAlias w8 = Session.withGap(lit(5).seconds()).on($("et")).as("w");

        // 使用窗口
        table.window(w8)
                .groupBy($("w"), $("id"))
                .select($("id"), $("id").count(), $("w").start(), $("w").end())
                .execute()
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
