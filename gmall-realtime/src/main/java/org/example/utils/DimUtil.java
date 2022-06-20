package org.example.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.example.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

// 查询维度的工具类
public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {

        // keypoint 优化1 旁路缓存 查询 Phoenix 前要先查询 Redis
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfoJsonStr = jedis.get(redisKey);

        if (dimInfoJsonStr != null) {
            // 重置TTL
            jedis.expire(redisKey, 24 * 60 * 60);

            // 关闭连接
            jedis.close();

            // 返回结果
            return JSON.parseObject(dimInfoJsonStr);
        }

        // 拼接查询语句
        // 别忘了单引号 注意空格
        String querySQL = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id = '" + id + "'";

        // 查询 Phoenix
        List<JSONObject> queryList = JDBCUtil.queryList(connection, querySQL, JSONObject.class, false);
        JSONObject dimInfoJson = queryList.get(0);

        // 返回结果之前，将数据写入Redis
        jedis.set(redisKey, dimInfoJson.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        // 一定要关闭连接
        jedis.close();

        // 返回结果
        return dimInfoJson;
    }

    // 更新维度时需要调用这个方法
    public static void delRedisDimInfo(String tableName, String id) {
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }
}
