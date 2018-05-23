package layer_speed;

import puffer.MyKafkaConsumer;

public class StreamingJob {

	private MyKafkaConsumer consumer = new MyKafkaConsumer();

	public void streamingJob() {

		try {
			String fileText = consumer.consume();
			System.out.println("Text: " + fileText);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// TODO
	}
}
