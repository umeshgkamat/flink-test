package com.umesh;

import com.umesh.tetstdata.Department;
import com.umesh.tetstdata.Employee;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class DataStreamTableJob {

    public static final String GROUP_ID = "mytest3";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        ObjectMapper objectMapper = new ObjectMapper();
        KafkaSource<String> employeeSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setTopics("employee")
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();
        KafkaSource<String> departmentSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setTopics("department")
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();


        SingleOutputStreamOperator<Employee> employeeSourceStreamOperator = env
                .fromSource(employeeSource, WatermarkStrategy.noWatermarks(), "Employee")
                .map(value -> objectMapper.readValue(value, Employee.class));
        Table employeeTable = tableEnvironment
                .fromDataStream(employeeSourceStreamOperator, $("deptId").as("empDeptId"),$("name").as("name"));

        SingleOutputStreamOperator<Department> departmentSingleOutputStreamOperator = env
                .fromSource(departmentSource, WatermarkStrategy.noWatermarks(), "Department")
                .map(value -> objectMapper.readValue(value, Department.class));

        Table departmentTable = tableEnvironment
                .fromDataStream(departmentSingleOutputStreamOperator, Schema.newBuilder().build());

        Table table = employeeTable.join(departmentTable)
                .where($("empDeptId").isEqual($("deptId")))
                .select($("name"), $("deptName"));

        DataStream<Row> rowDataStream =
                tableEnvironment.toChangelogStream(table);
        rowDataStream.sinkTo(new PrintSink<>());


        env.execute("Flink Java API Skeleton");
    }
}
