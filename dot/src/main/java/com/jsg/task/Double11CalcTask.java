package com.jsg.task;

import com.jsg.common.EasyRedisSink;
import com.jsg.entity.Order;
import com.jsg.map.CustomerMap;
import com.jsg.reduce.CustomRedisSink;
import com.jsg.util.CamelCaseUtil;
import com.jsg.util.client.KafkaUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.util.Properties;

/**
 * @Auther: sam
 * @Date: 2019/4/11
 * @Description:
 * @return
 */
public class Double11CalcTask {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置kafka连接参数
        Properties kafkaProp = KafkaUtil.getProperties();
        kafkaProp.setProperty("group.id", CamelCaseUtil.toUnderlineName(Double11CalcTask.class.getName()));

        //设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置检查点时间间隔
        env.enableCheckpointing(5000);
        //设置检查点模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //创建kafak消费者，获取kafak中的数据
        FlinkKafkaConsumer011<String> kafkaConsumer011 = new FlinkKafkaConsumer011<String>("double14", new SimpleStringSchema(), kafkaProp);
        //kafkaConsumer011.setStartFromEarliest();
        DataStreamSource<String> kafkaData = env.addSource(kafkaConsumer011);

        //解析kafka数据流 转化成固定格式数据流
        SingleOutputStreamOperator<Order> customerMapStream = kafkaData.map(new CustomerMap());
        //实例化Flink和Redis关联类FlinkJedisPoolConfig，设置Redis端口
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.136.131").setPassword("123456").build();
        //实例化RedisSink，并通过flink的addSink的方式将flink计算的结果插入到redis
        customerMapStream.addSink(new EasyRedisSink<Order>(conf,  new CustomRedisSink()));

        //设置程序名称
        env.execute(CamelCaseUtil.toUnderlineName(Double11CalcTask.class.getSimpleName()));
    }



}
