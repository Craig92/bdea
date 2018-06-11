package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MyFileReader {

	/**
	 * Read the file and copy the text in a String
	 * 
	 * @param file
	 *            the file object
	 * @return the text of the file
	 * @throws IOException
	 */
	public static String readFileForKafka(File file) throws IOException {

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));

			StringBuilder sb = new StringBuilder();
			String line = br.readLine();

			while (line != null) {
				sb.append(line);
				sb.append(System.lineSeparator());
				line = br.readLine();
			}
			return sb.toString();
		} finally {
			br.close();
		}

	}

	/**
	 * 
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static List<String[]> readFileForExport(File file) throws IOException {

		BufferedReader br = null;
		List<String[]> list = new ArrayList<>();
		String line;

		try {
			br = new BufferedReader(new FileReader(file));

			while ((line = br.readLine()) != null) {
				list.add(line.split("\t"));

			}
			return list;
		} finally {
			br.close();
		}
	}

	/**
	 * 
	 * @return
	 */
	public static int getNumberOfFiles() {

		int numbers = 0;
		File uploadDirectory = new File(App.destinationPath);

		for (File file : uploadDirectory.listFiles()) {
			if (file.isFile()) {
				numbers++;
			}
		}
		return numbers;
	}
}
