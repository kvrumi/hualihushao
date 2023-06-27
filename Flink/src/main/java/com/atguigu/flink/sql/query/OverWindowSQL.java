package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OverWindowSQL {

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

        // 基于行，上无边界到当前行
        String sql1 =
                " select id, vc, ts, " +
                        " sum(vc) over(partition by id order by pt rows between UNBOUNDED preceding and current row ) svc " +
                        " from t1 ";

        // 基于行，上有边界到当前行
        String  sql2 =
                " select id , vc, ts , "+
                        "   sum(vc) over(partition by id order by pt rows between 2 preceding and current row ) svc " +
                        " from t1 "  ;


        //基于时间
        //上无边界到当前时间
        String  sql3 =
                " select id , vc, ts , "+
                        "   sum(vc) over(partition by id order by et range between UNBOUNDED preceding and current row ) svc " +
                        " from t1 "  ;
        //上有边界到当前时间
        String  sql4 =
                " select id , vc, ts , "+
                        "   sum(vc) over(partition by id order by et range between interval '2' second preceding and current row ) svc " +
                        " from t1 "  ;


        //开窗，进行多次汇总
        String  sql5 =
                " select id, vc, ts, " +
                        " sum(vc) over w sumvc , " +
                        " max(vc) over w maxvc , " +
                        " min(vc) over w minvc " +
                        " from t1 " +
                        " window w as (partition by id order by et range between interval '2' second preceding and current row ) ";

        tableEnvironment.sqlQuery(sql5).execute().print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
