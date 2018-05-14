package layer_batch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

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

public class BatchJob extends TimerTask {

	private static int numberOfReducer = 2;
	private String hadoopHome = "C:\\Users\\Thorsten\\Entwicklung\\Bibliotheken\\Hadoop\\hadoop-2.8.3";

	public void batchJob(List<String> filenames) throws IOException, ClassNotFoundException, InterruptedException {

		System.setProperty("hadoop.home.dir", hadoopHome);

		Configuration conf = null;
		Job job = null;

		// Loescht temporaere Dateien
		for (String filename : filenames) {
			File file = new File(filename + "-temp");
			if (file.exists()) {
				file.delete();
			}
			file = new File(filename + "-result");
			if (file.exists()) {
				file.delete();
			}
		}

		// Zaehlt jedes Wort einmalig pro Dokument
		for (String filename : filenames) {
			conf = new Configuration();
			job = Job.getInstance(conf, "df counting");
			job.setJarByClass(BatchJob.class);
			job.setMapperClass(DocumentWordCounterMapper.class);
			job.setCombinerClass(DocumentWordCounterReducer.class);
			job.setReducerClass(DocumentWordCounterReducer.class);

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
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, new Path(filename + "-temp"));
			// TODO OutputFormat ==> NoSQL
			FileOutputFormat.setOutputPath(job, new Path(filename + "result"));
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.waitForCompletion(true);
		}
	}

	@Override
	public void run() {
		List<String> files = getFiles();
		try {
			if (files.size() != 0) {
				batchJob(files);
			}
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Get the uploaded files from the resorces directory
	 * 
	 * @return a list with the filepaths
	 */
	private List<String> getFiles() {

		File directory = new File("./src/main/resources");
		List<String> list = new ArrayList<>();

		if (directory.isDirectory()) {
			for (File file : directory.listFiles()) {
				if (file.isFile()) {
					list.add(file.getAbsolutePath());
				}
			}
			return list;
		} else {
			return new ArrayList<>();
		}
	}
}
