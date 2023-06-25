package com.atguigu.flink.func;

import com.atguigu.flink.pojo.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {

    @Override
    public WaterSensor map(String value) throws Exception {
        String[] strings = value.split(",");
        return new WaterSensor(
                strings[0].trim(),
                Long.valueOf(strings[1].trim()),
                Integer.valueOf(strings[2].trim())
        );
    }

}
