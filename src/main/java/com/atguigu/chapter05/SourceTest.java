package com.atguigu.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class SourceTest {
    public static void main(String[] args) throws Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从文本读取文件
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

        //2.从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(1);
        nums.add(2);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);

        //3.从Event类中读取数据
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary","./home",10001000L));
        events.add(new Event("Alice","./cart",2000L));
        events.add(new Event("Mary","./prod?id=100",3000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);

//        stream1.print("1");
//        numStream.print("nums");
//        stream2.print("2");

        //5. 从Kafka中读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop102:9092");
        DataStreamSource<String> kafkaStream = env.addSource(
                new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

        kafkaStream.print();

        env.execute();

    }
}
