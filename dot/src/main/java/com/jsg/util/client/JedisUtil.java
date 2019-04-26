package com.jsg.util.client;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

import java.util.ArrayList;
import java.util.List;

/**
 * @Auther: jiashaoguan
 * @Date: 2019/4/12
 * @Description:
 * @return
 */
public class JedisUtil {
    private static Log log =LogFactory.getLog(JedisUtil.class);
    private static String host = "192.168.136.131";
    private static Integer port = 6379;
    private static String password = "123456";

    static ShardedJedisPool shardedJedisPool;
    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(100);
        config.setMaxIdle(50);
        config.setMaxWaitMillis(3000);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        JedisShardInfo jedisShardInfo = new JedisShardInfo(host,port);
        jedisShardInfo.setPassword(password);
        List<JedisShardInfo> list = new ArrayList<JedisShardInfo>();
        list.add(jedisShardInfo);
        shardedJedisPool = new ShardedJedisPool(config,list);
    }
}
