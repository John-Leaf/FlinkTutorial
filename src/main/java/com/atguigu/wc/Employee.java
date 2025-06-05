package com.atguigu.wc;

public class Employee {
    private int id;
    private String name;
    private int age;
    private String location;
    private String legal;

    public Employee() {
    }

    public Employee(int id, String name, int age, String location, String legal) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.location = location;
        this.legal = legal;
    }

    @Override
    public String toString() {
        return "Employee{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", location='" + location + '\'' +
                ", legal='" + legal + '\'' +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
