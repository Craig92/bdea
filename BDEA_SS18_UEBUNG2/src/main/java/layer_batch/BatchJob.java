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

import database.MyCassandraCluster;
import main.App;
import main.MyFileReader;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BatchJob extends TimerTask {

	private static int numberOfReducer = 2;
	private String hadoopHome = "C:\\Users\\Thorsten\\Entwicklung\\Bibliotheken\\Hadoop\\hadoop-2.8.3";

	/**
	 * Counting the document frequencies using hadoop
	 * 
	 * @param filenames
	 *            the path of the files
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public void batchJob(List<String> filenames) throws IOException, ClassNotFoundException, InterruptedException {

		System.setProperty("hadoop.home.dir", hadoopHome);

		// Loescht temporaere Dateien
		File file = new File(App.destinationPath + "-df-result");
		if (file.exists()) {
			FileUtils.deleteDirectory(file);
		}

		// Zaehlt jedes Wort einmalig pro Dokument
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "df counting");

		job.setJarByClass(BatchJob.class);
		job.setMapperClass(DocumentWordCounterMapper.class);
		job.setCombinerClass(DocumentWordCounterReducer.class);
		job.setReducerClass(DocumentWordCounterReducer.class);

		job.setNumReduceTasks(numberOfReducer);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(App.destinationPath));
		FileOutputFormat.setOutputPath(job, new Path(App.destinationPath + "-df-result"));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.waitForCompletion(true);

		// Speichert Ergebnisse in Datenbank
		MyCassandraCluster cluster = new MyCassandraCluster();
		List<String[]> list = new ArrayList<>();
		for (File temp : new File(App.destinationPath + "-df-result").listFiles()) {
			if (temp.getName().contains("part-r-") && !temp.getName().contains(".crc")) {
				list = MyFileReader.readFileForExport(temp);
				for (String[] array : list) {
					cluster.writeDFIntoDatabase(array[0], Integer.parseInt(array[1]));
				}
			}
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
			e.printStackTrace();
		}
	}

	/**
	 * Get the uploaded files from the resources directory
	 * 
	 * @return a list with the path of the files
	 */
	private List<String> getFiles() {

		File directory = new File(App.destinationPath);
		List<String> list = new ArrayList<>();

		if (directory.isDirectory()) {
			for (File file : directory.listFiles()) {
				if (file.isFile()) {
					list.add(file.getAbsolutePath());
				}
			}
			return list;
		} else {
			return list;
		}
	}
}
