import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.util.Properties;

/**
 * @Auther: jiashaoguan
 * @Date: 2019/4/12
 * @Description:
 * @return
 */
public class FlinkSqlWindowsUserPv {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        TableConfig tableConfig = new TableConfig();
        StreamTableEnvironment tableEnv = new StreamTableEnvironment(env, tableConfig) {
            @Override
            public StreamExecutionEnvironment execEnv() {
                return super.execEnv();
            }

            @Override
            public StreamQueryConfig queryConfig() {
                return super.queryConfig();
            }
        };

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.136.131:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "FlinkSqlWindowsUserPv");

        FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<String>("test", new SimpleStringSchema(), properties);
        DataStream<String> stream = env.addSource(kafkaConsumer);
        DataStream<Tuple5<String,String,String,String,Long>> map = stream.map(new MapFunction<String, Tuple5<String, String, String, String, Long>>() {
            @Override
            public Tuple5<String, String, String, String, Long> map(String s) throws Exception {
                String[] split = s.split(" ");

                return new Tuple5<String,String,String,String,Long>(split[0],split[1],split[2],split[3],Long.valueOf(split[4]) * 1000);
            }
        });

        map.print();
        
        tableEnv.registerDataStream("user", map, "userId,itemId,categoryId,behavior,timestampin,proctime.proctime");
        Table sqlQuery = tableEnv.sqlQuery("SELECT TUMBLE_END(proctime, INTERVAL '10' SECOND) as processtime,"
                        + "userId,count(*) as pvcount "
                        + "FROM Users "
                        + "GROUP BY TUMBLE(proctime, INTERVAL '10' SECOND), userId"
                       );


        DataStream<Tuple3<Timestamp, String, Long>> appendStream = tableEnv.toAppendStream(sqlQuery, Types.TUPLE(Types.SQL_TIMESTAMP,Types.STRING,Types.LONG));
        appendStream.print();

//sink to mysql
        appendStream.map(new MapFunction<Tuple3<Timestamp,String,Long>, UserPvEntity>() {

            private static final long serialVersionUID = -4770965496944515917L;

            @Override
            public UserPvEntity map(Tuple3<Timestamp, String, Long> value) throws Exception {

                return new UserPvEntity(Long.valueOf(value.f0.toString()),value.f1,value.f2);
            }
        }).addSink(new SinkUserPvToMySQL2());

        env.execute("userPv from Kafka");



    }
}
