package main;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSFileWriter {

	public void writeFileToHDFS(String fileName) throws FileNotFoundException, IllegalArgumentException, IOException {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		FileSystem hdfs = FileSystem.get(conf);

		FileStatus[] fsStatus = hdfs.listStatus(new Path("/"));
		for (int i = 0; i < fsStatus.length; i++) {
			System.out.println(fsStatus[i].getPath().toString());
		}

		// benötigt Schreibrechte!
		OutputStream out = hdfs.create(new Path(fileName));
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
		br.write("Hello World");
		br.close();

		hdfs.close();

	}

}
