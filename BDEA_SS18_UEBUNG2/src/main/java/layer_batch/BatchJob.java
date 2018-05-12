package layer_batch;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import layer_serving.WholeWordCounterMapper;
import layer_serving.WholeWordCounterReducer;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BatchJob {

	private static int numberOfReducer = 2;

	public void batchJob(String[] filenames) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = null;
		Job job = null;

		// Zaehlt jedes Wort einmalig pro Dokument
		for (String filename : filenames) {
			conf = new Configuration();
			job = Job.getInstance(conf, "df counting");
			job.setJarByClass(BatchJob.class);
			job.setMapperClass(DocumentWordCounterMapper.class);
			job.setCombinerClass(DocumentWordCounterReducer.class);
			job.setReducerClass(null);

			job.setNumReduceTasks(numberOfReducer);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, new Path(filename));
			FileOutputFormat.setOutputPath(job, new Path(filename + "-temp"));
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.waitForCompletion(true);
		}

		// Zaehlt Werte aller Dokumente zusammen
		for (String filename : filenames) {
			conf = new Configuration();
			job = Job.getInstance(conf, "dfs");

			job.setJarByClass(BatchJob.class);
			job.setMapperClass(WholeWordCounterMapper.class);
			job.setCombinerClass(WholeWordCounterReducer.class);
			job.setReducerClass(WholeWordCounterReducer.class);

			job.setNumReduceTasks(numberOfReducer);
			job.setOutputKeyClass(null);
			job.setOutputValueClass(null);

			FileInputFormat.addInputPath(job, new Path(filename + "-temp"));
			// TODO OutputFormat ==> NoSQL
			job.waitForCompletion(true);
		}
	}
}
