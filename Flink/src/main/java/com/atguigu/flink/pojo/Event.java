package com.atguigu.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Event {
    // 用户
    private String user;
    // 点击的url
    private String url;
    // 何时点击的
    private Long timestamp;
}
