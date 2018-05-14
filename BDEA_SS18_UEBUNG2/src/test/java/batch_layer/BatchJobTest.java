package batch_layer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import layer_batch.BatchJob;

public class BatchJobTest {

	BatchJob bjob = new BatchJob();
	List<String> files = new ArrayList<>();

	@Test
	public void test() throws ClassNotFoundException, IOException, InterruptedException {
		files.add("./src/main/resources/test.txt");
		bjob.batchJob(files);
	}
}
