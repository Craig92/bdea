package layer_speed;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import main.App;

public class StreamingJob {

	public void streamingJob(String filename) {

		try {
			//TODO 
			// MyCassandraCluster cluster = new MyCassandraCluster();
			// MyKafkaConsumer consumer = new MyKafkaConsumer();
			// String fileText = consumer.consume();
			// fileText = fileText.replace("[^A-Za-z ]", "");

			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

			DataStream<Tuple2<String, Integer>> dataStream = env
					.socketTextStream(App.SERVERS.substring(0, App.SERVERS.length() - 5), 9092)

					.flatMap((String sentence, Collector<Tuple2<String, Integer>> out) -> {
						for (String word : sentence.split(" ")) {
							out.collect(new Tuple2<String, Integer>(word, 1));
						}
					}).keyBy(0).sum(1);

			dataStream.print();
			System.out.println("Ausgabe");
			env.execute();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
