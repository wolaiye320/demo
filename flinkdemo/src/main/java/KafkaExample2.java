import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaExample2 {
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
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        // 创建消费者
        FlinkKafkaConsumer011 consumer = new FlinkKafkaConsumer011<String>(
                sourceTopic,
                new SimpleStringSchema(),
                p);
        // 设置读取最早的数据
//        consumer.setStartFromEarliest();

        // 读取Kafka消息
        DataStream<String> input = env.addSource(consumer);


        // 数据处理
      /*  DataStream<String> result = input.map(new MapFunction<String, String>() {
            public String map(String s) throws Exception {
                String msg = "Flink study ".concat(s);
                System.out.println(msg);
                return msg;
            }
        });*/

        DataStream<Tuple2<String, Integer>> countsStream = input.map(new LineSplitter()).keyBy(0).sum(1);
        countsStream.print();

        // 创建生产者
       /* FlinkKafkaProducer producer = new FlinkKafkaProducer<String>(
                sinkTopic,
                new KeyedSerializationSchemaWrapper<String>(new KafkaMsgSchema()),
                p,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);*/

        // 将数据写入Kafka指定Topic中
        //result.addSink(producer);

        // 执行job
        try {
            env.execute("Kafka Example");
        } catch (InterruptedException  e) {
            e.printStackTrace();
        }catch (Exception  e) {
            throw e;
        }
    }

    public static final class LineSplitter implements MapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<String, Integer> map(String s) throws Exception {
            JSONObject object = JSONObject.parseObject(createRowJson(s));
            String message = object.getString("message");
            Integer price = Integer.parseInt(message.split(",")[1]);
            return new Tuple2<String, Integer>("price", price);
        }
    }

        public static String createRowJson(String rowString) {
            String jsonString = "{ message: \""  + rowString + "\"}";
            return jsonString;
        }
}