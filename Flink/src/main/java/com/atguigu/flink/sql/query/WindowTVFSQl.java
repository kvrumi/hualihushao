package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class WindowTVFSQl {

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

        tableEnvironment.createTemporaryView("t1",table);

        //分组窗口
        //滚动
        //处理时间滚动窗口
        String sql1 =
                " select " +
                        " id , " +
                        " sum(vc) , " +
                        " window_start , " +
                        " window_end , " +
                        " from table ( " +
                        " Tumble ( table t1, descriptor(pt), interval '5' second ) ) " +
                        " group by window_start, window_end, id ";

        //事件时间滚动窗口
        String sql2 =
                " select " +
                        " id , " +
                        " sum(vc) , " +
                        " window_start , " +
                        " window_end , " +
                        " from table ( " +
                        " Tumble ( table t1, descriptor(et), interval '5' second ) ) " +
                        " group by window_start, window_end, id ";


        //滑动
        //处理时间滑动窗口
        String sql3 =
                " select " +
                        "   id , " +
                        "   sum(vc) ," +
                        "   window_start ," +
                        "   window_end " +
                        " from TABLE( " +
                        "   HOP( TABLE t1 , DESCRIPTOR(pt) , INTERVAL '5' SECOND , INTERVAL '10' SECOND ) " +
                        " ) " +
                        " group by window_start ,window_end , id " ;
        //事件时间滑动窗口
        String sql4 =
                " select " +
                        "   id , " +
                        "   sum(vc) ," +
                        "   window_start ," +
                        "   window_end " +
                        " from TABLE( " +
                        "   HOP( TABLE t1 , DESCRIPTOR(et) , INTERVAL '5' SECOND , INTERVAL '10' SECOND ) " +
                        " ) " +
                        " group by window_start ,window_end , id " ;


        // 累积
        //处理时间累积窗口
        String sql5 =
                " select " +
                        "   id , " +
                        "   sum(vc) ," +
                        "   window_start ," +
                        "   window_end " +
                        " from TABLE( " +
                        "   CUMULATE( TABLE t1 , DESCRIPTOR(pt) , INTERVAL '2' SECOND , INTERVAL '10' SECOND ) " +
                        " ) " +
                        " group by window_start ,window_end , id " ;

        //事件时间累积窗口
        String sql6 =
                " select " +
                        "   id , " +
                        "   sum(vc) ," +
                        "   window_start ," +
                        "   window_end " +
                        " from TABLE( " +
                        "   CUMULATE( TABLE t1 , DESCRIPTOR(et) , INTERVAL '2' SECOND , INTERVAL '10' SECOND ) " +
                        " ) " +
                        " group by window_start ,window_end , id " ;

        //使用窗口
        tableEnvironment.sqlQuery(sql5).execute().print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
