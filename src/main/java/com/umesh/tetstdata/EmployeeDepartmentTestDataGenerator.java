package com.umesh.tetstdata;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class EmployeeDepartmentTestDataGenerator {

    public static void main(String[] args) {
        Random random = new Random();

        List<Department> departments = new ArrayList<>();
        for (long i = 0; i < 10; i++) {
            departments.add(new Department(i + 1, "Department " + (i + 1)));
        }

        List<Employee> employees = new ArrayList<>();
        for (long i = 0; i < 50; i++) {
            long empid = i + 1;
            String name = "Employee " + empid;
            long deptid = random.nextInt(10) + 1; // Randomly assign department ID between 1 and 100

            employees.add(new Employee(empid, name, deptid));
        }

        for (Employee employee : employees) {
            Department department = getDepartmentById(departments, employee.getDeptId());
            //employee.setDepartment(department);
        }

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        KafkaProducer<Long, Department> deptProducer = new KafkaProducer<Long, Department>(properties);
        KafkaProducer<Long, Employee> employeeProducer = new KafkaProducer<Long, Employee>(properties);

        System.out.println("Departments:");
        for (Department department : departments) {
            System.out.println(department);
            deptProducer.send(new ProducerRecord<>("department", department.getDeptId(), department));
        }

        System.out.println("\nEmployees:");
        for (Employee employee : employees) {
            System.out.println(employee);
            employeeProducer.send(new ProducerRecord<>("employee", employee.getEmpId(), employee));
        }
    }

    private static Department getDepartmentById(List<Department> departments, Long deptid) {
        for (Department department : departments) {
            if (department.getDeptId() == deptid) {
                return department;
            }
        }
        return null;
    }
}