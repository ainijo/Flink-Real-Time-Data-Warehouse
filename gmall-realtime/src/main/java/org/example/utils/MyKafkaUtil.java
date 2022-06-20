package org.example.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {

    // 定义brokers和默认主题名称
    private static String brokers = "node01:9092,node02:9092,node03:9092";
    private static String default_topic = "DWD_DEFAULT_TOPIC";

    // 获取KafkaProducer，该方法用于ods层
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(
                brokers,
                topic,
                new SimpleStringSchema()
        );
    }

    // 获取含有泛型的KafkaProducer
    // 方法的泛型前面也要加
    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        // properties 表示一个持久的属性集
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        return new FlinkKafkaProducer<T>(
                default_topic,
                // 自定义 Schema 我们的主题就是从这里传的
                kafkaSerializationSchema,
                properties,
                // 用于指定一致性语义 注意 EXACTLY_ONCE 只有当开启 ckp 时生效
                // todo 开启 ckp 时，使用 EXACTLY_ONCE 会报错
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    // 获取Kafka消费者
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                properties
        );
    }

    // 拼接kafka相关属性到DDL
    public static String getKafkaDDL(String topic, String groupId) {
        return  " 'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + brokers + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset'  ";
    }
}
