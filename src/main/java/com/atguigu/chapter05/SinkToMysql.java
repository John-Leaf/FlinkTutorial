package com.atguigu.chapter05;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class SinkToMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./home", 4000L),
                new Event("Alice", "./cart", 5000L),
                new Event("Bob", "./prod?id=100", 6000L));

        stream.addSink(JdbcSink.<Event>sink(
                        "INSERT INTO clicks (user,url) value (?,?)",
                        ((statement, event) -> {
                            statement.setString(1, event.user);
                            statement.setString(2, event.url);
                        }),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/test")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("exs9R^VN")
                        .build()
        ));
        env.execute();
    }

}

