package batch_layer;

import java.io.IOException;

import org.junit.Test;

import layer_batch.BatchJob;

public class BatchJobTest {

	BatchJob bjob = new BatchJob();
	String[] files = { "./src/main/resources/test.txt" };

	@Test
	public void test() throws ClassNotFoundException, IOException, InterruptedException {
		bjob.batchJob(files);
	}
}
