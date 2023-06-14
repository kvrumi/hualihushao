package com.atguigu.flink.func;

import com.atguigu.flink.pojo.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ClickSource implements SourceFunction<Event> {

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        String[] users = {"Zhangs", "Lisi", "Tom", "Jerry", "Peiqi"};
        String[] urls = {"/home", "/cart", "detail", "pay"};
        Random random = new Random();
        while (isRunning) {
            Event event = new Event(users[random.nextInt(users.length)], urls[random.nextInt(urls.length)], System.currentTimeMillis());
            sourceContext.collect(event);

            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
