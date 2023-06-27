package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

public class OverWindowTableAPI {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new WaterSensor(fields[0].trim(), Long.valueOf(fields[1].trim()), Integer.valueOf(fields[2].trim()));
                        }

                );
        //创建表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        Schema schema = Schema.newBuilder()
                .column("id", "STRING")
                .column("vc", "INT")
                .column("ts", "BIGINT")
                .columnByExpression("pt", "PROCTIME()")
                .columnByExpression("et", "TO_TIMESTAMP_LTZ(ts,3)")
                .watermark("et", "et - INTERVAL '1' SECOND")
                .build();
        Table table = tableEnvironment.fromDataStream(ds, schema);

        // 基于行，上无边界下当前行
        //OVER RANGE FOLLOWING windows are not supported yet.
        OverWindow w1 =
                Over.partitionBy($("id")).orderBy($("pt")).preceding(UNBOUNDED_ROW).following(CURRENT_ROW).as("w");
        // 基于行，上有边界到当前行
        OverWindow w2 = Over.partitionBy($("id")).orderBy($("pt")).preceding(lit(2).seconds()).following(CURRENT_ROW).as("w");
        //基于时间
        //上无边界到当前时间
        OverWindow w5 =
                Over.partitionBy($("id")).orderBy($("et")).preceding(UNBOUNDED_RANGE).following(CURRENT_RANGE).as("w");
        //上有边界到当前时间
        OverWindow w6 =
                Over.partitionBy($("id")).orderBy($("et")).preceding(lit(2).seconds()).following(CURRENT_RANGE).as("w");

        table.window(w5)
                .select($("id"),$("id").count().over($("w")).as("cnt"))
                .execute()
                .print();


        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
