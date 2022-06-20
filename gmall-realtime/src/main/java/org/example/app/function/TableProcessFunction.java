package org.example.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.bean.TableProcess;
import org.example.common.GmallConfig;

import java.sql.*;
import java.util.Arrays;
import java.util.List;


// 为了方便后续的处理，主流的输出同样采用 JSONObject 类型
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private OutputTag<JSONObject> objectOutputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection;

    // 使用构造方法把参数传进我们定义的函数中
    public TableProcessFunction(OutputTag<JSONObject> objectOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.objectOutputTag = objectOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    // keypoint 针对两个流启动顺序的数据丢失问题 可以在open方法中建表 防止数据丢失
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    // value: {"db":"", "tn":"", "before":{}, "after":{}, "type":""}
    // 这里解析的是 after 数据
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        // 广播流
        // 1.解析数据 String -> TableProcess
        // todo 因为过滤了delete，目前的after中一定有数据，后续补充功能时要注意判断after是否为空
        JSONObject jsonObject = JSON.parseObject(value);
        String data = jsonObject.getString("after");
        // Json 解析合并了下划线方式和小驼峰命名方式
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        // 2.校验表是否存在，不存在则建表
        // 只有Hbase才需要建表
        // fixme 为了防止数据丢失 这部分操作可以放在 open 中
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(
                    tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPK(),
                    tableProcess.getSinkExtend()
            );
        }

        // 3.写入状态并广播
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }

    // 建表语句：create table if not exists db.tn(id varchar primary key, ...)
    private void checkTable(String sinkTable, String sinkColumns, String sinkPK, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        try {
           // 首先对没有主键和没有扩展字段的情况做处理
           if (sinkPK == null) {
               sinkPK = "id";
           }
           if (sinkExtend == null) {
               // 如果不做这步处理的话字符串最后会拼个null
               sinkExtend = "";
           }

           StringBuffer createTableSQL = new StringBuffer("create table if not exists ")
                   .append(GmallConfig.HBASE_SCHEMA)
                   .append(".")
                   .append(sinkTable)
                   .append("(");

           String[] fields = sinkColumns.split(",");

           for (int i = 0; i < fields.length; i++) {
               String field = fields[i];
               createTableSQL.append(field).append(" varchar");
               // 判断是否为主键
               if (sinkPK.equals(field)) {
                   createTableSQL.append(" primary key");
               }

               // 判断是否为最后一个字段，如果不是则添加一个逗号
               if (i < fields.length - 1) {
                   createTableSQL.append(", ");
               }
           }

           // 拼接额外字段
           createTableSQL.append(")").append(sinkExtend);

           // 打印建表语句
           System.out.println(createTableSQL);

           // 预编译SQL
           preparedStatement = connection.prepareStatement(createTableSQL.toString());

           // 执行
           preparedStatement.execute();
        } catch (SQLException e) {
           // 建表异常说明整个程序都走不下去了
           throw new RuntimeException(sinkTable + "建表失败");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    // value: {"db":"", "tn":"", "before":{}, "after":{}, "type":""}
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 主流
        // 1.读取状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 拼接 key，这里注意拼接的字符要和状态广播时拼接的字符一致
        String key = value.getString("tableName") + "-" + value.getString("type");
        // 可能为 null
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
            // 2.过滤字段
            JSONObject data = value.getJSONObject("after");
            filterColumn(
                    data,
                    tableProcess.getSinkColumns()
            );

            // 3.分流
            // 将输出表或者输出主题信息追加到 value 中
            value.put("sinkTable", tableProcess.getSinkTable());

            // 分流操作
            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                // Kafka数据写入主流
                out.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                // hbase数据写入phoenix
                ctx.output(objectOutputTag, value);
            }

        } else {
            System.out.println("该组合Key " + key + " 不存在！");
        }
    }

    /*
     * @param data          {"id":"", ...}
     * @param sinkColumn    id,...
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] fields = sinkColumns.split(",");
        // 把fields转换为集合
        List<String> columns = Arrays.asList(fields);

        // 把json数据转换为迭代器
        // Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();

        // while (iterator.hasNext()) {
        //    Map.Entry<String, Object> next = iterator.next();
        //    // 不存在对应的数据时，则去掉
        //    if (!columns.contains(next.getKey())) {
        //        iterator.remove();
        //    }
        //}

        // 不存在对应的数据时，则去掉这些数据
        data.entrySet().removeIf(next -> !columns.contains(next.getKey()));
    }
}
