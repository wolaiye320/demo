package com.jsg.common;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
/**
 * @Auther: sam
 * @Date: 2019/4/12
 * @Description:
 * @return
 */
public class EasyRedisSink<IN> extends RichSinkFunction<IN> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.flink.streaming.connectors.redis.RedisSink.class);
    private String additionalKey;
    private EasyRedisMapper<IN> redisSinkMapper;
    private RedisCommand redisCommand;
    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisCommandsContainer redisCommandsContainer;

    public EasyRedisSink(FlinkJedisConfigBase flinkJedisConfigBase, EasyRedisMapper<IN> redisSinkMapper) {
        Preconditions.checkNotNull(flinkJedisConfigBase, "Redis connection pool config should not be null");
        Preconditions.checkNotNull(redisSinkMapper, "Redis Mapper can not be null");
        Preconditions.checkNotNull(redisSinkMapper.getCommandDescription(), "Redis Mapper data type description can not be null");
        this.flinkJedisConfigBase = flinkJedisConfigBase;
        this.redisSinkMapper = redisSinkMapper;
        RedisCommandDescription redisCommandDescription = redisSinkMapper.getCommandDescription();
        this.redisCommand = redisCommandDescription.getCommand();

    }

    public void invoke(IN input) throws Exception {
        this.additionalKey = this.redisSinkMapper.getRecordKeyFromData(input);
        String key = this.redisSinkMapper.getKeyFromData(input);
        String value = this.redisSinkMapper.getValueFromData(input);
        switch(this.redisCommand) {
            case RPUSH:
                this.redisCommandsContainer.rpush(key, value);
                break;
            case LPUSH:
                this.redisCommandsContainer.lpush(key, value);
                break;
            case SADD:
                this.redisCommandsContainer.sadd(key, value);
                break;
            case SET:
                this.redisCommandsContainer.set(key, value);
                break;
            case PFADD:
                this.redisCommandsContainer.pfadd(key, value);
                break;
            case PUBLISH:
                this.redisCommandsContainer.publish(key, value);
                break;
            case ZADD:
                this.redisCommandsContainer.zadd(this.additionalKey, value, key);
                break;
            case HSET:
                HashMap hashMap = this.redisSinkMapper.getKeyValueMapFromData(input);

                for (Object mapkey : hashMap.keySet()) {

                    this.redisCommandsContainer.hset(this.additionalKey, mapkey.toString(), hashMap.get(mapkey).toString());
                }
                break;
            default:
                throw new IllegalArgumentException("Cannot process such data type: " + this.redisCommand);
        }

    }

    public void open(Configuration parameters) throws Exception {
        this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkJedisConfigBase);
    }

    public void close() throws IOException {
        if (this.redisCommandsContainer != null) {
            this.redisCommandsContainer.close();
        }

    }
}
