package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class GroupWindowSQL {

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

        tableEnvironment.createTemporaryView("t1", table);

        //分组窗口
        //滚动
        //处理时间滚动窗口
        String sql1 =
                "select id," +
                        "sum(vc)," +
                        "Tumble_start(pt,interval '5' second)," +
                        "Tumble_end(pt,interval '5' second)" +
                        "from t1" +
                        "group by Tumble(pt,interval '5' second),id";
        //事件时间滚动窗口
        String sql2 =
                " select " +
                        "   id , " +
                        "   sum(vc) ," +
                        "   TUMBLE_START(et , INTERVAL '5' SECOND) , " +
                        "   TUMBLE_END(et , INTERVAL '5' SECOND)  " +
                        " from t1 " +
                        " group by TUMBLE( et , INTERVAL '5' SECOND ) , id ";


        //滑动
        //处理时间滑动窗口
        String sql3 =
                " select " +
                        "   id , " +
                        "   sum(vc) ," +
                        "   HOP_START(pt , INTERVAL '5' SECOND  ,INTERVAL '10' SECOND) , " +
                        "   HOP_END(pt , INTERVAL '5' SECOND  ,INTERVAL '10' SECOND)  " +
                        " from t1 " +
                        " group by HOP( pt , INTERVAL '5' SECOND  ,INTERVAL '10' SECOND ) , id ";
        //事件时间滑动窗口
        String sql4 =
                " select " +
                        "   id , " +
                        "   sum(vc) ," +
                        "   HOP_START(et , INTERVAL '5' SECOND  ,INTERVAL '10' SECOND) , " +
                        "   HOP_END(et , INTERVAL '5' SECOND  ,INTERVAL '10' SECOND)  " +
                        " from t1 " +
                        " group by HOP( et , INTERVAL '5' SECOND  ,INTERVAL '10' SECOND ) , id ";
        //会话
        //处理时间会话窗口
        String sql5 =
                " select " +
                        "   id , " +
                        "   sum(vc) ," +
                        "   SESSION_START(pt , INTERVAL '5' SECOND) , " +
                        "   SESSION_END(pt , INTERVAL '5' SECOND)  " +
                        " from t1 " +
                        " group by SESSION( pt , INTERVAL '5' SECOND) , id ";

        //事件时间会话窗口
        String sql6 =
                " select " +
                        "   id , " +
                        "   sum(vc) ," +
                        "   SESSION_START(et , INTERVAL '5' SECOND) , " +
                        "   SESSION_END(et , INTERVAL '5' SECOND)  " +
                        " from t1 " +
                        " group by SESSION( et , INTERVAL '5' SECOND) , id ";
        //使用窗口
        tableEnvironment.sqlQuery(sql5).execute().print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
