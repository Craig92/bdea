package main;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.common.IOUtils;

public class Main {

	public String writeFileIntoHDFS(File file) throws IOException {
		
		//Source file in the local file system
		String localSrc = file.getAbsolutePath();
		//Destination file in HDFS
		String dst = "";

		//Input stream for the file in local file system to be written to HDFS
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

		//Get configuration of Hadoop system
		Configuration conf = new Configuration();
		System.out.println("Connecting to -- "+conf.get("fs.defaultFS"));

		//Destination file in HDFS
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		OutputStream out = fs.create(new Path(dst));

		//Copy file from local to HDFS
		IOUtils.copyBytes(in, out, 4096, true);

		System.out.println(dst + " copied to HDFS");
		
		return null;
		
	}
}
