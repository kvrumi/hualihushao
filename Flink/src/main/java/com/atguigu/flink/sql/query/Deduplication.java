package com.atguigu.flink.sql.query;

import com.atguigu.flink.func.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Deduplication {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> ds = env.addSource(new ClickSource());

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Schema schema =
                Schema.newBuilder()
                        .column("user", "string")
                        .column("url", "string")
                        .column("ts", "bigint")
                        .columnByExpression("pt", "proctime()")
                        .columnByExpression("et", "to_timestamp_ltz(ts, 3)")
                        .watermark("et", "et - interval '1' second")
                        .build();
        Table table = tableEnv.fromDataStream(ds, schema);
        table.printSchema();
        tableEnv.createTemporaryView("t1", table);

        //求每个窗口中每个url最后到达的数据
        //1. 使用窗口补充窗口信息
        String windowSql =
                " select " +
                        " from TABLE( " +
                        " Tumble(Table t1, DESCRIPTOR(et), Interval '10' second) )";

        Table table1 = tableEnv.sqlQuery(windowSql);
        tableEnv.createTemporaryView("t2", table1);

        //2. 基于每条数据的时间求排名
        String rankSql =
                " select url, et , ws , we , " +
                        " row_number() over(partition by url , ws, we order by et desc ) rk " +
                        " from t2 ";

        Table table2 = tableEnv.sqlQuery(rankSql);
        tableEnv.createTemporaryView("t3", table2);

        //3. 取topN
        String topNSql =
                " select url , et ,ws , we , " +
                        " from t3 " +
                        " where rk = 1 ";

        tableEnv.sqlQuery(topNSql).execute().print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}





















