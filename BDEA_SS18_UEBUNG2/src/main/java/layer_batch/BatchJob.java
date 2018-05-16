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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.tomcat.util.http.fileupload.FileUtils;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BatchJob extends TimerTask {

	private static int numberOfReducer = 2;
	private String hadoopHome = "C:\\Users\\Thorsten\\Entwicklung\\Bibliotheken\\Hadoop\\hadoop-2.8.3";
	public static String destinationPath = "C:/Users/Thorsten/Git/BDEA/BDEA_SS18_UEBUNG2/src/main/resources/uploads";

	public void batchJob(List<String> filenames) throws IOException, ClassNotFoundException, InterruptedException {

		System.setProperty("hadoop.home.dir", hadoopHome);

		Configuration conf = null;
		Job job = null;

		// Loescht temporaere Dateien
		File file = new File(destinationPath + "-df-result");
		if (file.exists()) {
			FileUtils.deleteDirectory(file);
		}

		// Zaehlt jedes Wort einmalig pro Dokument

		conf = new Configuration();
		job = Job.getInstance(conf, "df counting");
		job.setJarByClass(BatchJob.class);
		job.setMapperClass(DocumentWordCounterMapper.class);
		job.setCombinerClass(DocumentWordCounterReducer.class);
		job.setReducerClass(DocumentWordCounterReducer.class);

		job.setNumReduceTasks(numberOfReducer);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(destinationPath));
		FileOutputFormat.setOutputPath(job, new Path(destinationPath + "-df-result"));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.waitForCompletion(true);

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

		File directory = new File(destinationPath);
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
