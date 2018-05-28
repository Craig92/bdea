package layer_speed;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import puffer.MyKafkaConsumer;

public class StreamingJob {

	private MyKafkaConsumer consumer = new MyKafkaConsumer();

	public void streamingJob() {

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			// TODO
			env.execute();
			String fileText = consumer.consume();
			System.out.println("Text: " + fileText);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// TODO
	}
}
