package com.atguigu.flink.state;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IStateTTL {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("hadoop102",8888)
                ;

    }

}
