package org.example.app.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.common.GmallConfig;
import org.example.utils.DimUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 加载驱动
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        // 设置自动提交
        connection.setAutoCommit(true);
    }

    /*
        value:
        {
            "database":"",
            "tableName":"",
            "before":{"id":"","tm_name":""....},
            "after":{"id":"","tm_name":""....},
            "type":"c u d",
            "sinkTable":"" (写入之前追加的字段)
        }
     */
    // SQL upsert into db.tn(id, ...) values(..., ...)  没有考虑到值的类型问题
    // SQL upsert into db.tn(id, ...) values('...', '...')
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        PreparedStatement preparedStatement = null;

        try {
            // 获取SQL语句
            String sinkTable = value.getString("sinkTable");
            String id = value.getJSONObject("after").getString("id");
            String upsertSQL = getupsertSQL(sinkTable, value.getJSONObject("after"));
            System.out.println(upsertSQL);

            // 预编译SQL
            preparedStatement = connection.prepareStatement(upsertSQL);


            // keypoint 进程不同步导致 Redis 中存在脏数据
            // 写入前需要进行判断
            // 如果当前数据为 update 操作，则删除Redis中的数据
            if ("update".equals(value.getString("type"))) {
                DimUtil.delRedisDimInfo(sinkTable.toUpperCase(), id);
            }

            // 执行插入操作
            preparedStatement.executeUpdate();

            // 除了自动提交 也可以设置手动提交
            // connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    private String getupsertSQL(String sinkTable, JSONObject data) {
        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();
        // 由于存在类型的问题，所以要给值前后加单引号
        // 这里可以将分隔符视为 ', 来实现这一过程
        // 注意前后的单引号不能丢
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(keySet, ",") + ") values(" + "'" +
                StringUtils.join(values, "','") + "'" + ")";
    }
}
