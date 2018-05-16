package layer_batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DocumentWordCounterMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	List<String> list = new ArrayList<String>();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		StringTokenizer itr = null;

		itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			String temp = itr.nextToken();
			if (!list.contains(temp)) {
				word.set(temp);
				context.write(word, one);
				list.add(temp);
			}
		}
	}
}
