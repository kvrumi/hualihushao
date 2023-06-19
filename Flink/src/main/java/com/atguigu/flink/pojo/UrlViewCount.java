package com.atguigu.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UrlViewCount {

    private Long start;
    private Long end;
    private String url;
    private Long count;

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "[" + start +
                "- " + end +
                "), url='" + url + '\'' +
                ", count=" + count +
                '}';
    }

}
