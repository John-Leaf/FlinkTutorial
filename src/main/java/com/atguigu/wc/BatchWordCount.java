package com.atguigu.wc;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.Int;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BatchWordCount {
    public static void main(String[] args) throws Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从文件读取数据
        DataStreamSource<String> lineDataSource = env.readTextFile("input/words.txt");


        //3.将每行数据进行分词，转换成而元组类型

        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDataSource.flatMap((String line, Collector<String> words) -> {
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                }).returns(Types.STRING)
                .map(word -> Tuple2.of(word, System.currentTimeMillis()))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> value, long l) {
                                return value.f1;
                            }
                        }));

        //4.按照word进行分组
//        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOne.keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .aggregate(new WordCountAgg());

        //5.分组内进行聚合统计
//        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

        //6.打印结果
        result.print();

        //程序执行
        env.execute();
    }

    public static class WordCountAgg implements AggregateFunction<Tuple2<String,Long>,Tuple2<String,Long>,Tuple2<String,Long>>{
        @Override
        public Tuple2<String, Long> createAccumulator() {
            return Tuple2.of(null,null);
        }

        @Override
        public Tuple2<String, Long> add(Tuple2<String, Long> value, Tuple2<String, Long> acumulator) {
            return Tuple2.of(acumulator.f0, acumulator.f1 + 1);
        }

        @Override
        public Tuple2<String, Long> getResult(Tuple2<String, Long> acumulator) {
            return acumulator;
        }

        @Override
        public Tuple2<String, Long> merge(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> acc1) {
            return null;
        }
    }
}
