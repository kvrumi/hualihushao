package com.atguigu.flink.sql.query;

import com.atguigu.flink.func.ClickSource;
import com.atguigu.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TopN {

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

        //Url点击的TopN
        //1. 使用窗口求每个url的点击次数
        String countSql =
                " select url, count(url) cnt, window_start, window_end " +
                        " from TABLE(" +
                        " Tumble(Table t1, Descriptor(et), interval '10' second) " +
                        " ) " +
                        " group by url, window_start, window_end ";

        Table table1 = tableEnv.sqlQuery(countSql);
        tableEnv.createTemporaryView("t2", table1);

        //2. 基于求出来的点击次数排序求排名
        String rankSql =
                " select url , cnt , window_start ,window_end , " +
                        " row_number() over(partition by window_start, window_end order by cnt desc) rnk " +
                        " from t2 ";

        Table table2 = tableEnv.sqlQuery(rankSql);
        tableEnv.createTemporaryView("t3", table2);

        String topNSql =
                " select url , cnt , window_start, window_end , rnk " +
                        " from t3 " +
                        " where rnk < 3 ";

        Table table3 = tableEnv.sqlQuery(topNSql);

        table3.execute().print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }

}
