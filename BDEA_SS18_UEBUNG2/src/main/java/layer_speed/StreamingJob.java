package layer_speed;

import puffer.MyKafkaConsumer;

public class StreamingJob {

	private MyKafkaConsumer consumer = new MyKafkaConsumer();

	public void streamingJob() {

		try {
			System.out.println("Result: " + consumer.consume());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// TODO
	}
}
