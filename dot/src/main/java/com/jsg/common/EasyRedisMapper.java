package com.jsg.common;

/**
 * @Auther: sam
 * @Date: 2019/4/12
 * @Description:
 * @return
 */
import java.io.Serializable;
import java.util.HashMap;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;

public interface EasyRedisMapper<T> extends Function, Serializable {
    RedisCommandDescription getCommandDescription();

    String getRecordKeyFromData(T var1);
    String getKeyFromData(T var1);
    String getValueFromData(T var1);
    HashMap getKeyValueMapFromData(T var1);
}
