package com.atguigu.wc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FlatMapTest {
    public static void main(String[] args) {
        FlatMapTest flatMapTest = new FlatMapTest();
        flatMapTest.test();
    }
    public void test() {
        List<Employee> employees = Arrays.asList(
                new Employee(1, "张三", 23, "郑州", "合法"),
                new Employee(2, "李四", 25, "合肥", "合法"),
                new Employee(3, "王五", 26, "青岛", "合法"),
                new Employee(4, "王二麻子", 27, "上海", "合法"),
                new Employee(5, "赵子龙", 28, "北京", "合法")
        );
        List<List<String>> collect = employees.stream().map(employee -> {
            List<String> list = new ArrayList<>();
            list.add(employee.getName());
            return list;
        }).collect(Collectors.toList());
        System.out.println(collect);
    }
}
