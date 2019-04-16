import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerDemo {
	public static void main(String[] args) {


		Properties props = new Properties();
		//kafka broker节点
		props.put("bootstrap.servers", "192.168.136.131:9092");
		props.put(ENABLE_AUTO_COMMIT_CONFIG, "true");
		//kafka消费组
		props.put(GROUP_ID_CONFIG, "test_consumer_group1");
		props.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
		props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
		//kerberos 角色
		//props.put("sasl.kerberos.service.name", "kafka");
		props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		//props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
		KafkaConsumer consumer = new KafkaConsumer<String, String>(props);

		//System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
		//System.setProperty("sun.security.krb5.debug", "true");
		//kafka topic

		/*consumer.subscribe(Collections.singleton("double11"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);

			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());

			}

		}*/
	}
}