package com.jsg.reduce;

import com.jsg.common.EasyRedisMapper;
import com.jsg.entity.Order;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;

import java.util.HashMap;

/**
 * @Auther: jiashaoguan
 * @Date: 2019/4/12
 * @Description:
 * @return
 */
public class CustomRedisSink implements EasyRedisMapper<Order> {
    //设置数据使用的数据结构 HashSet 并设置key的名称

    public RedisCommandDescription getCommandDescription() {

        return new RedisCommandDescription(RedisCommand.HSET, "" );
    }

    public String getRecordKeyFromData(Order order) {
        String keyPrefix = "custom";
        return keyPrefix + ":" + order.getCustomId();
    }


    /**
     * 获取 value值 value的数据是键值对
     * @param
     * @return
     */
    //指定key
    public String getKeyFromData(Order order) {
        return null;
    }
    //指定value
    public String getValueFromData(Order order) {
        return null;
    }

    public HashMap getKeyValueMapFromData(Order order) {

        HashMap keyValueMap = new HashMap();
        keyValueMap.put("CustomId",order.getCustomId());
        keyValueMap.put("Price",order.getPrice());
        keyValueMap.put("CityId",order.getCityId());
        keyValueMap.put("Item",order.getItemId());
        return keyValueMap;
    }
}
