package com.jsg.controller;

import com.jsg.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Auther: sam
 * @Date: 2019/4/12
 * @Description:
 * @return
 */

@RestController
public class OrderController {
    protected Logger log = LoggerFactory.getLogger(OrderController.class);
    private Order order;

@RequestMapping("/index")
    public ModelMap index() {
        order = new Order();
        order.setCustomId("40060886");
        order.setCityId("88");
        order.setItemId("358305");
        order.setPrice(Long.parseLong("6454"));
        ModelMap mp = new ModelMap();
        mp.put("CustomId",order.getCustomId());
        return mp;

    }
}
