package com.atguigu.chapter06;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;

public class WindowProcesstest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1).getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));
        stream.print("input");
        stream.keyBy(date -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .process(new UvCountByWindow())
                                .print();

        env.execute();

    }
    //实现自定义的ProcessWindowFunction，输出一条统计信息

    public static class UvCountByWindow extends ProcessWindowFunction<Event,String, Boolean, TimeWindow>{
        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            HashSet<String> userSet = new HashSet<>();
            //使用forEach方法遍历
            elements.forEach(element -> userSet.add(element.user));
            //使用iterator遍历
//            Iterator<Event> iterator = elements.iterator();
//            while (iterator.hasNext()){
//                userSet.add(iterator.next().user);
//            }
            //使用for循环遍历
//            for (Event element : elements) {
//                userSet.add(element.user);
//            }

            int uv = userSet.size();
            long start = context.window().getStart();
            long end = context.window().getEnd();
            out.collect("窗口"+ new Timestamp(start) + "~" + new Timestamp(end) + "的uv值是：" + uv);

        }
    }

}
