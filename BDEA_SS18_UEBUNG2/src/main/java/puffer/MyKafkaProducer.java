package puffer;

import java.io.File;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import main.App;
import main.MyFileReader;

public class MyKafkaProducer {

	private static Properties props;

	public MyKafkaProducer() {

		props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, App.SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "BdeaProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");

	}

	/**
	 * Add a file to the kafka cluster
	 * 
	 * @param file
	 */
	public void produce(File file) {

		try {
			Producer<String, String> producer = new KafkaProducer<String, String>(props);
			ProducerRecord<String, String> rec = new ProducerRecord<String, String>(App.TOPIC,
					MyFileReader.readFileForKafka(file));
			System.out.printf("Producer Record:(%d, %s)\n", rec.key(), rec.value());
			producer.send(rec);
			producer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
