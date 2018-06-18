package layer_speed;

import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import main.App;

@SuppressWarnings("deprecation")
public class StreamingJob {

	/**
	 * 
	 * @param filename
	 */
	public void streamingJob(String filename) {

		try {

			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			ParameterTool parameterTool = ParameterTool.fromSystemProperties();
			Properties props = parameterTool.getProperties();
			props.put("bootstrap.servers", App.SERVERS);
			DataStream<String> messageStream = env
					.addSource(new FlinkKafkaConsumer011<>("jweis", new SimpleStringSchema(), props));

			DataStream<Tuple1<String>> dataStream = messageStream

					.flatMap(new FlatMapFunction<String, Tuple1<String>>() {
						private static final long serialVersionUID = 1L;

						@Override
						public void flatMap(String line, Collector<Tuple1<String>> out) throws Exception {
							String[] words = line.replaceAll("[^a-zA-ZäöüÄÖÜß ]", "").split("\\s+");
							for (String word : words) {
								System.out.println(word);
								out.collect(new Tuple1<String>(filename + " " + word.toLowerCase()));
							}
						}
					}).keyBy(0).timeWindowAll(Time.seconds(30)).sum(1);
			// ohne die shuffle FUnktion wird nur das erste Wort des Textes hinzugefügt
			// mit Shuffle werden zumindest teile hinzugefügt und gezählt

			// Zusatz für DF können wir .keyBy(0) verwenden
			// bei diesem aufruf wird jedes wort 1 mal gezählt
			;

			dataStream.print();
			CassandraSink.addSink(dataStream).setQuery("UPDATE jweis.tf SET tf = tf + 1 WHERE word = ?;")
					.setHost("hoppy.informatik.hs-mannheim.de", 9042).build();
			env.execute();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
