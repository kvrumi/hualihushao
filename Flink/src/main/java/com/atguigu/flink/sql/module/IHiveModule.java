package com.atguigu.flink.sql.module;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.module.hive.HiveModule;

import java.util.Arrays;

public class IHiveModule {

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
                .column("vc" , "int")
                .column("ts" , "bigint")
                .columnByExpression("pt" , "proctime()")
                .columnByExpression("et" , "to_timestamp_ltz(ts, 3)")
                .watermark("et" , "et - interval '1' second")
                .build();

        Table table = tableEnv.fromDataStream(ds1, schema);
        tableEnv.createTemporaryView("t1" , table);

        //自定义split函数
        //引入HiveModule
        HiveModule hiveModule = new HiveModule("3.1.3", IHiveModule.class.getClassLoader());
        tableEnv.loadModule("hive",hiveModule);

        //模块的使用顺序
        tableEnv.useModules("hive");

        //查看Flink支持的Module
        System.out.println(Arrays.toString(tableEnv.listModules()));

        //使用 split 函数
        // split("a,bb,ccc,dddd" , ",") =>  [a , bb , ccc , dddd]
        tableEnv.sqlQuery("select split('a,bb,ccc,dddd' , ',')").execute().print();
        //卸载module
        //tableEnv.unloadModule("hive");

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
