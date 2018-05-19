package puffer;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.web.multipart.MultipartFile;

import main.App;

public class MyKafkaConsumer {

	private static Properties props;

	public MyKafkaConsumer() {

		props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, App.SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "BDEAConsumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
	}

	/**
	 * 
	 * @throws InterruptedException
	 */
	public void consume() throws InterruptedException {

		Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Collections.singletonList(App.TOPIC));

		ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
		consumerRecords.forEach(record -> {
			System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", record.key(), record.value(), record.partition(),
					record.offset());
		});
		consumer.commitAsync();
		Thread.sleep(500);

		consumer.close();

	}

}
