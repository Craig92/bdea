package layer_speed;

import puffer.MyKafkaConsumer;

public class StreamingJob {

	private MyKafkaConsumer consumer = new MyKafkaConsumer();

	public void streamingJob() {

		try {
			consumer.consume();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// TODO
	}
}
