package com.jsg.controller;

import com.jsg.entity.Order;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

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


@RequestMapping(value = "/produceOrdor", method = RequestMethod.GET)
    public void produceOrdor (@RequestParam String customer, @RequestParam String price, @RequestParam String city, @RequestParam String item) {
    String orderStr = customer+","+price+","+city+","+item;
    Properties props = new Properties();
    props.put("bootstrap.servers", "192.168.136.131:9092");
    props.put("acks", "all");
    props.put("retries", 1);
    props.put("batch.size", 1);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    Producer<String, String> kafkaProducer = new KafkaProducer<>(props);
    kafkaProducer.send(new ProducerRecord<>("double14", orderStr), new Callback() {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            //do sth
        }
    });


    }

}
