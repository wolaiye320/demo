package com.jsg.map;

import com.jsg.entity.Order;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Auther: jiashaoguan
 * @Date: 2019/4/12
 * @Description:
 * @return
 */
public class CustomerMap implements MapFunction<String, Order> {

    public Order map(String s) throws Exception {
        String[] split = s.split(",");
        String customId = split[0].replace("customer","");
        long price = Long.parseLong(split[1]);
        String cityId = split[2].replace("city","");
        String itemId = split[3].replace("item","");
        Order userInfo  = new Order(customId+"", price, cityId, itemId);

        return userInfo;
    }
}
