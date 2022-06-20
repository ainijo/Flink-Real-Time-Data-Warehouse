package org.example.utils;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.example.bean.TransientSink;
import org.example.common.GmallConfig;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {

    public static <T> SinkFunction<T> getSink(String sql) {
        return JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        // getDeclaredFields可以获取所有的属性信息
                        Field[] fields = t.getClass().getDeclaredFields();

                        // 遍历字段
                        // keypoint 防止字段错位，重新定义一个下标
                        // int j = 1;
                        // keypoint 这里也可以用一个变量记录注解的数量，然后向前推进
                        int offset = 0;
                        for (int i = 0; i < fields.length; i++) {
                            try {
                                // 获取字段
                                Field field = fields[i];

                                // 给预编译sql对象赋值
                                // 设置私有属性可以访问
                                field.setAccessible(true);

                                // 获取字段上的注解
                                TransientSink annotation = field.getAnnotation(TransientSink.class);
                                if (annotation != null) {
                                    // 如果存在该注解，则不写出去
                                    // keypoint 这种方式会导致下标错位，因为 i 是递增的 可以通过偏移量解决
                                    offset ++;
                                    continue;
                                }

                                // 根据字段获取值
                                // keypoint 反射 顾名思义 反着来 以前是拿对象去调用get方法获取字段的值，现在反过来用字段去get对象的值
                                // 补充：对于方法来说 反射的思路就是 obj.method(args) => method.invoke(obj, args)
                                Object value = field.get(t);

                                // 考虑到注解的存在，新用一个变量进行赋值
                                // preparedStatement.setObject(j, value);
                                // j += 1;

                                // 注意 index 是 i + 1
                                // keypoint 采用偏移量的方式解决这一问题
                                preparedStatement.setObject(i + 1 - offset, value);

                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );
    }

}
