package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RegularJoin {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> ds1 = env.socketTextStream("hadoop102", 8888)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new WaterSensor(fields[0].trim(), Long.valueOf(fields[1].trim()), Integer.valueOf(fields[2].trim()));
                        }

                );
        SingleOutputStreamOperator<WaterSensor> ds2 = env.socketTextStream("hadoop102", 9999)
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new WaterSensor(fields[0].trim(), Long.valueOf(fields[1].trim()), Integer.valueOf(fields[2].trim()));
                        }
                );
        //流转表
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //配置状态的过期时间
        tableEnv.getConfig().getConfiguration().setLong("table.exec.state.ttl", 10 * 1000);

        Schema schema = Schema.newBuilder()
                .column("id", "string")
                .column("vc" , "int")
                .column("ts" , "bigint")
                .columnByExpression("pt" , "proctime()")
                .columnByExpression("et" , "to_timestamp_ltz(ts, 3)")
                .watermark("et" , "et - interval '1' second")
                .build();
        Table left = tableEnv.fromDataStream(ds1, schema);
        Table right = tableEnv.fromDataStream(ds2, schema);
        tableEnv.createTemporaryView("t1" , left);
        tableEnv.createTemporaryView("t2" , right) ;

        //内联结
        String innerSql =
                "select l.id , l.vc , r.id , r.vc " +
                        " from t1 l inner join t2 r " +
                        " on l.id = r.id";
        //tableEnv.sqlQuery(innerSql).execute().print();

        //外联结
        //左外 | 右外
        String outerSql =
                " select l.id, l.vc , r.id , r.vc " +
                        " from t1 l left outer join t2  r" +
                        " on l.id = r.id";
        //tableEnv.sqlQuery(outerSql).execute().print();

        //全外
        String fullOuterSql =
                " select l.id, l.vc , r.id , r.vc " +
                        " from t1 l full outer join t2  r" +
                        " on l.id = r.id";

        tableEnv.sqlQuery(fullOuterSql).execute().print();
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}













