import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import java.util.Properties;

public class KafkaExample {
    public static void main(String[] args) throws Exception {
        // 用户参数获取
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        // Stream 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source的topic
        String sourceTopic = "flink-topic";
        // Sink的topic
        String sinkTopic = "flink-topic-output";
        // broker 地址
        String broker = "192.168.136.131:9092";

        // 属性参数 - 实际投产可以在命令行传入
        Properties p = parameterTool.getProperties();
        p.putAll(parameterTool.getProperties());
        p.put("bootstrap.servers", broker);
        p.put("group.id", "test_consumer_group1");

        env.getConfig().setGlobalJobParameters(parameterTool);

        // 创建消费者
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<String>(
                sourceTopic,
                new KafkaMsgSchema(),
                p);
        // 设置读取最早的数据
//        consumer.setStartFromEarliest();

        // 读取Kafka消息
        DataStream<String> input = env.addSource(consumer);


        // 数据处理
        DataStream<String> result = input.map(new MapFunction<String, String>() {
            public String map(String s) throws Exception {
                String msg = "Flink study ".concat(s);
                System.out.println(msg);
                return msg;
            }
        });

        // 创建生产者
        FlinkKafkaProducer producer = new FlinkKafkaProducer<String>(
                sinkTopic,
                new KeyedSerializationSchemaWrapper<String>(new KafkaMsgSchema()),
                p,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        // 将数据写入Kafka指定Topic中
        result.addSink(producer);

        // 执行job
        env.execute("Kafka Example");
    }
}