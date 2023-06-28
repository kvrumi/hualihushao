package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class LookupJoin {

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

        //流转表
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema schema = Schema.newBuilder()
                .column("id", "string")
                .column("vc", "int")
                .column("ts", "bigint")
                .columnByExpression("pt", "proctime()")
                .columnByExpression("et", "to_timestamp_ltz(ts, 3)")
                .watermark("et", "et - interval '1' second")
                .build();
        Table left = tableEnv.fromDataStream(ds1, schema);
        tableEnv.createTemporaryView("t1", left);

        //Mysql Connector
        String readSql =
                " creat table t2 ( " +
                        " id String , " +
                        " vc Int , " +
                        " ts Bigint " +
                        " ) with ( " +
                        " 'connector' = 'jdbc' , " +
                        " 'url' = 'jdbc:mysql://hadoop102:3306/test' , " +
                        " 'table-name' = 't2' , " +
                        " 'username' = 'root' , " +
                        " 'password' = '000000' ) ";

        tableEnv.executeSql(readSql);

        //维表联结
        // 流中每来一条数据 ，都要与Mysql中的维度表进行一次关联
        String lookupSql =
                " select " +
                        " from t1 l " +
                        " join t2 for system_time as of l.pt as r " +
                        " on l.id = r.id ";

        tableEnv.sqlQuery(lookupSql).execute().print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
