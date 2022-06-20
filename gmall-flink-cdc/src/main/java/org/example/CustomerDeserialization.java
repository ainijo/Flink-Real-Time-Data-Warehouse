package org.example;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.fastjson.JSONObject;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class CustomerDeserialization implements DebeziumDeserializationSchema<String> {

    /*
        原始的数据形式
        SourceRecord{
            sourcePartition={server=mysql_binlog_source},
            sourceOffset={file=mysql-bin.000001, pos=746}}
            ConnectRecord{topic='mysql_binlog_source.gmall-flink.base_trademark', kafkaPartition=null, key=Struct{id=11}, keySchema=Schema{mysql_binlog_source.gmall_flink.base_trademark.Key:STRUCT}, value=Struct{after=Struct{id=11,tm_name=香奈儿,logo_url=/static/default.jpg},source=Struct{version=1.4.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=0,snapshot=last,db=gmall-flink,table=base_trademark,server_id=0,file=mysql-bin.000001,pos=746,row=0},op=c,ts_ms=1653269271956}, valueSchema=Schema{mysql_binlog_source.gmall_flink.base_trademark.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
        封装的数据格式
        {
            "database":"",
            "tableName":"",
            "before":{"id":"","tm_name":""....},
            "after":{"id":"","tm_name":""....},
            "type":"c u d",
            // "ts": 156456135615
        }
     */

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        // 1.创建Json对象用于存储最终的数据
        JSONObject result = new JSONObject();

        // 2.获取库名和表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];

        // 2.5 先将值转为Struct形式，方便后续处理
        Struct value = (Struct) sourceRecord.value();
        // 3.获取"before"数据
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        // 考虑到 insert 操作没有 before，所以要加判断条件
        if (before != null){
            // 获取before中的字段
            Schema beforeSchema = before.schema();
            List<Field> beforeFields = beforeSchema.fields();
            for (Field field : beforeFields) {
                Object beforeValue = before.get(field);
                beforeJson.put(field.name(), beforeValue);
            }
        }

        // 4.获取"after"数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        // 考虑到 Delete 操作没有 after，所以要加判断条件
        if (after != null){
            // 获取after中的字段
            Schema afterSchema = after.schema();
            List<Field> afterFields = afterSchema.fields();
            for (Field field : afterFields) {
                Object afterValue = after.get(field);
                afterJson.put(field.name(), afterValue);
            }
        }

        // 5.获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        // 6.字段写入Json对象
        result.put("database", database);
        result.put("tableName", tableName);
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("type", type);

        // 7.输出数据
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        // 这部分和源码保持一制
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
