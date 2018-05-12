package puffer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MyKafkaProducer {

	public static void produce() {
		  final String TOPIC = "test";
		    final String BOOTSTRAP_SERVERS = "localhost:9092";

		    Properties props = new Properties();
	        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
	        props.put(ProducerConfig.CLIENT_ID_CONFIG, "BdeaProducer");
	        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
	        		"org.apache.kafka.common.serialization.StringSerializer");
	        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
	        		"org.apache.kafka.common.serialization.StringSerializer");

	        Producer<String, String> producer = new KafkaProducer<String, String>(props);
	        ProducerRecord<String, String> rec = new ProducerRecord<String, String>(
	        		TOPIC, "Hello World!");
	        producer.send(rec);
	        producer.close();

	}

}
