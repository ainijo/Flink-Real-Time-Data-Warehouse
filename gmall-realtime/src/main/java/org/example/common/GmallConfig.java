package org.example.common;

// 保存对应的常量
public class GmallConfig {
    // Phoenix 库名
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";

    // Phoenix 驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix 连接参数
    // 这里的2181端口是 zookeeper 的
    public static final String PHOENIX_SERVER = "jdbc:phoenix:node01,node02,node03:2181";

    //ClickHouse Url
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://node01:8123/default";

    //ClickHouse 驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
}
