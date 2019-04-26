
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Auther: jiashaoguan
 * @Date: 2019/4/12
 * @Description:
 * @return
 */


public class StreamJoinStaticData {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment envStream = StreamExecutionEnvironment.createLocalEnvironment();
        envStream.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        Properties propsConsumer = new Properties();
        propsConsumer.setProperty("bootstrap.servers", "");
        propsConsumer.setProperty("group.id", "test");
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>("topic-test", new SimpleStringSchema(), propsConsumer);
        consumer.setStartFromLatest();
        DataStream<String> stream = envStream.addSource(consumer).setParallelism(2);
        stream.print();
        DataStream<Tuple2<Tuple2<String, String>, Integer>> tuple2Stream = stream.map(s -> {
            JSONObject jsonObject = JSON.parseObject(s);
            String fromCity = jsonObject.getString("fromCity");
            String toCity = jsonObject.getString("toCity");
            Integer ticketNum = jsonObject.getInteger("ticketNum");
            return Tuple2.of(Tuple2.of(fromCity, toCity), ticketNum);
        }).returns(Types.TUPLE(Types.TUPLE(Types.STRING, Types.STRING), Types.INT));

        //超时时间设置长一些,要不容易报错.,这步相当于建立一个读Es表的流
        DataStream<Tuple2<Tuple2<String, String>, String>> dimTable = AsyncDataStream.unorderedWait(tuple2Stream, new AsyncEsDataRequest(), 2, TimeUnit.SECONDS, 100);
        //实时流Join Es表的流
        DataStream<Tuple3<Tuple2<String, String>, String, Integer>> finalResult = tuple2Stream.join(dimTable).where(new FirstKeySelector()).equalTo(new SecondKeySelector())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1000))).apply(
                        new JoinFunction<Tuple2<Tuple2<String, String>, Integer>, Tuple2<Tuple2<String, String>, String>, Tuple3<Tuple2<String, String>, String, Integer>>() {
                            @Override
                            public Tuple3<Tuple2<String, String>, String, Integer> join(Tuple2<Tuple2<String, String>, Integer> first, Tuple2<Tuple2<String, String>, String> second) throws Exception {
                                return Tuple3.of(first.f0, second.f1, first.f1);
                            }
                        }
                );

        finalResult.print();

        envStream.execute("this-test");
    }

    private static class FirstKeySelector implements KeySelector<Tuple2<Tuple2<String, String>, Integer>, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> getKey(Tuple2<Tuple2<String, String>, Integer> value) throws Exception {
            return value.f0;
        }
    }

    private static class SecondKeySelector implements KeySelector<Tuple2<Tuple2<String, String>, String>, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> getKey(Tuple2<Tuple2<String, String>, String> value) throws Exception {
            return value.f0;
        }
    }

}
