package layer_speed;

import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

			DataStream<Tuple2<String, Integer>> dataStream = messageStream

					.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

						/**
						* 
						*/
						private static final long serialVersionUID = 1L;

						@Override
						public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
							String[] words = line.split("\\W+");
							for (String word : words) {
								System.out.println(word);
								out.collect(new Tuple2<String, Integer>(filename + " " + word.toLowerCase(), 1));
							}
						}
					})

					.keyBy(0).sum(1);

			System.out.println("Ausgabe");
			dataStream.print();
			CassandraSink.addSink(dataStream).setQuery("INSERT INTO jweis.tf(word, tf) values (?, ?);")
					.setHost("hoppy.informatik.hs-mannheim.de", 9042).build();

			env.execute();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
