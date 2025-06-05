package com.atguigu.chapter06;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.Tuple2;

import java.util.HashSet;

public class WindowAggregateTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1).getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));
        stream.print();

        //所有数据设置相同的key，发送到同一分区
        stream.keyBy(date -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .aggregate(new AvgPv())
                .print();

        env.execute();
    }
    public static class AvgPv implements AggregateFunction<Event, Tuple2<HashSet<String>,Long>,Double>{
        @Override
        public Tuple2<HashSet<String>, Long> createAccumulator() {
            return Tuple2.apply(new HashSet<String>(),0L);
        }

        @Override
        public Tuple2<HashSet<String>, Long> add(Event event, Tuple2<HashSet<String>, Long> accumulator) {
            accumulator._1.add(event.user);
            return Tuple2.apply(accumulator._1,accumulator._2+1);
        }

        @Override
        public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
            return (double) accumulator._2 / accumulator._1.size();
        }

        @Override
        public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> hashSetLongTuple2, Tuple2<HashSet<String>, Long> acc1) {
            return null;
        }
    }
}
