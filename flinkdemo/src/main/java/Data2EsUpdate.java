import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.http.HttpHost;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

/**
 * @Auther: jiashaoguan
 * @Date: 2019/4/12
 * @Description:
 * @return
 */
public class Data2EsUpdate {
    public static void main(String[] args) throws Exception {
        String kafkaBrokers = null;
        String zkBrokers = null;
        String topic = null;
        String groupId = null;
        if (args.length == 4) {
            kafkaBrokers = args[0];
            zkBrokers = args[1];
            topic = args[2];
            groupId = args[3];
        } else {
            System.exit(1);
        }
        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置kafka连接参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBrokers);
        properties.setProperty("zookeeper.connect", zkBrokers);
        properties.setProperty("group.id", groupId);
        //设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置检查点时间间隔
        env.enableCheckpointing(5000);
        //创建kafak消费者，获取kafak中的数据
        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer010.setStartFromEarliest();
        DataStreamSource<String> kafkaData = env.addSource(kafkaConsumer010);
        kafkaData.print();
        //解析kafka数据流 转化成固定格式数据流
        DataStream<Tuple5<Long, Long, Long, String, Long>> userData = kafkaData.map(new MapFunction<String, Tuple5<Long, Long, Long, String, Long>>() {
            @Override
            public Tuple5<Long, Long, Long, String, Long> map(String s) throws Exception {
                Tuple5<Long, Long, Long, String, Long> userInfo=null;
                String[] split = s.split(",");
                if(split.length!=5){
                    System.out.println(s);
                }else {
                    long userID = Long.parseLong(split[0]);
                    long itemId = Long.parseLong(split[1]);
                    long categoryId = Long.parseLong(split[2]);
                    String behavior = split[3];
                    long timestamp = Long.parseLong(split[4]);
                    userInfo = new Tuple5<>(userID, itemId, categoryId, behavior, timestamp);
                }
                return userInfo;
            }
        });
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("ip", 9200, "http"));
        ElasticsearchSink.Builder<Tuple5<Long, Long, Long, String, Long>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple5<Long, Long, Long, String, Long>>() {


                    public UpdateRequest updateIndexRequest(Tuple5<Long, Long, Long, String, Long> element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("userid", element.f0.toString());
                        json.put("itemid", element.f1.toString());
                        json.put("categoryid", element.f2.toString());
                        json.put("behavior", element.f3);
                        json.put("timestamp", element.f4.toString());
                        UpdateRequest updateRequest=new UpdateRequest();
                        updateRequest.index("flink").type("user").id(json.get("userid")).doc(json).upsert(Requests.indexRequest()
                                .index("flink")
                                .type("user")
                                .source(json));
                        return updateRequest;
                    }


                    @Override
                    public void process(Tuple5<Long, Long, Long, String, Long> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(updateIndexRequest(element));
                    }
                }
        );
        /*     必须设置flush参数     */
        //刷新前缓冲的最大动作量
        esSinkBuilder.setBulkFlushMaxActions(1);
        //刷新前缓冲区的最大数据大小（以MB为单位）
        esSinkBuilder.setBulkFlushMaxSizeMb(500);
        //论缓冲操作的数量或大小如何都要刷新的时间间隔
        esSinkBuilder.setBulkFlushInterval(5000);
        userData.addSink(esSinkBuilder.build());
        env.execute("data2es");
    }
}
