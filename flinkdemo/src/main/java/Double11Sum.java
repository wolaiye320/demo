import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @Auther: sam
 * @Date: 2019/4/9
 * @Description:
 * @return
 */
public class Double11Sum {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Properties properties = new Properties();

    public void consumeKafkaData() {
        try {
        properties.put("bootstrap.servers", "192.168.136.131:9092");

        //kafka消费组
        properties.put("group.id", "test_consumer_group1");
        FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<String>("double14", new SimpleStringSchema(), properties);
        DataStream<String> dataStream = env.addSource(kafkaConsumer);
        DataStream<Tuple2<String, Integer>> countsStream = dataStream.flatMap(new LineSplitter()).keyBy(0).sum(1);
        countsStream.print();
        env.execute("Double 11 Real Time Transaction Volume");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //统计总的实时交易额
    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {

            if (value != null) {
                JSONObject object = JSONObject.parseObject(createRowJson(value));
                String message = object.getString("message");
                Integer price = Integer.parseInt(message.split(",")[1]);
                out.collect(new Tuple2<String, Integer>("price", price));
            }

        }
    }

    public static String createRowJson(String rowString) {
        String jsonString = "{ message: \""  + rowString + "\"}";
        return jsonString;
    }

    public static void main(String[] args) {
        Double11Sum double11Sum = new Double11Sum();
        double11Sum.consumeKafkaData();

    }
}
