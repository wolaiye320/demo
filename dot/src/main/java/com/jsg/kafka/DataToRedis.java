package com.jsg.kafka;

import com.jsg.common.EasyRedisMapper;
import com.jsg.common.EasyRedisSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;


import java.util.HashMap;
import java.util.Properties;

/**
 * @Auther: sam
 * @Date: 2019/4/11
 * @Description:
 * @return
 */
public class DataToRedis {
    public static void main(String[] args) throws Exception {
        String bootstrapServers = "192.168.136.131:9092";
        String zkBrokers = "192.168.136.131:2181";
        String topic = "double14";
        String groupId = "DataToRedis";

        String redisHost = "192.168.136.131";

        args = new String[]{"--bootstrap.servers", bootstrapServers, "--zookeeper.connect", zkBrokers, "--topic", topic, "--group.id" + groupId };

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() != 4) {
            System.out.println("Missing parameters!\n" +
                    "Usage: "
                    + "--bootstrap.servers <kafka brokers> "
                    + "--zookeeper.connect <zk quorum> "
                    + "--topic <topic> "
                    + "--group.id <some id>");
            return;
        }
        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置kafka连接参数
        Properties properties = parameterTool.getProperties();

        //设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置检查点时间间隔
        env.enableCheckpointing(5000);
        //设置检查点模式
        // env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        System.out.println("===============》 开始读取kafka中的数据  ==============》");
        //创建kafak消费者，获取kafak中的数据
        FlinkKafkaConsumer011<String> kafkaConsumer011 = new FlinkKafkaConsumer011<String>(topic, new SimpleStringSchema(), properties);
        //kafkaConsumer011.setStartFromEarliest();
        DataStreamSource<String> kafkaData = env.addSource(kafkaConsumer011);
        kafkaData.print();
        //解析kafka数据流 转化成固定格式数据流
        SingleOutputStreamOperator<Tuple4<Long, Long, String, String>> userData = kafkaData.map(new KafkaMapper());
        //实例化Flink和Redis关联类FlinkJedisPoolConfig，设置Redis端口
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost(redisHost).setPassword("123456").build();
        //实例化RedisSink，并通过flink的addSink的方式将flink计算的结果插入到redis
        userData.addSink(new EasyRedisSink<Tuple4<Long, Long, String, String>>(conf,  new RedisMapperCustom()));
        System.out.println("===============》 flink任务结束  ==============》");
        //设置程序名称
        env.execute("data_to_redis_db0");
    }


    public static  class KafkaMapper implements MapFunction<String, Tuple4<Long, Long, String, String>> {
        public Tuple4<Long, Long, String, String> map(String s) throws Exception {
            String[] split = s.split(",");
            long customId = Long.parseLong(split[0].replace("customer",""));
            long price = Long.parseLong(split[1]);
            String cityId = split[2];
            String itemId = split[3];
            Tuple4<Long, Long, String, String> userInfo = new Tuple4<Long, Long, String, String>(customId, price, cityId, itemId);
            return userInfo;
        }
    }

    //指定Redis key并将flink数据类型映射到Redis数据类型
    public static class RedisMapperCustom implements EasyRedisMapper<Tuple4<Long, Long, String, String>> {

        //设置数据使用的数据结构 HashSet 并设置key的名称

        public RedisCommandDescription getCommandDescription() {

            return new RedisCommandDescription(RedisCommand.HSET, "" );
        }

        public String getRecordKeyFromData(Tuple4<Long, Long, String, String> data) {
            String keyPrefix = "custom";
            return keyPrefix + ":" + data.f0.toString();
        }


        /**
         * 获取 value值 value的数据是键值对
         * @param data
         * @return
         */
        //指定key
        public String getKeyFromData(Tuple4<Long, Long, String, String> data) {
            return null;
        }
        //指定value
        public String getValueFromData(Tuple4<Long, Long, String, String> data) {
            return null;
        }

        public HashMap getKeyValueMapFromData(Tuple4<Long, Long, String, String> data) {

            HashMap keyValueMap = new HashMap();
            keyValueMap.put("CustomId",data.f0);
            keyValueMap.put("Price",data.f1);
            keyValueMap.put("CityId",data.f2.replace("city",""));
            keyValueMap.put("Item",data.f3.replace("item",""));
            return keyValueMap;
        }
    }


}
