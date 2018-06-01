package database;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class MyCassandraCluster {

	private Cluster cluster;

	public MyCassandraCluster() {
		cluster = Cluster.builder().addContactPoint("hoppy.informatik.hs-mannheim.de").withPort(9042).build();
	}

	public void writeDFIntoDatabase(String word, int df) {
		Session session = cluster.connect();
		session.execute("INSERT INTO jweis.df (word,value) " + "VALUES('" + word + "'," + df + ");");

		session.close();
	}

	public void writeTFIntoDatabase(String word, int tf) {
		Session session = cluster.connect();
		session.execute("INSERT INTO jweis.tf (word,value) " + "VALUES('" + word + "'," + tf + ");");

		session.close();
	}

	public int readDFFromDatabase(String word) {
		int ret = 0;
		Session session = cluster.connect();
		ResultSet results = session.execute("SELECT * FROM jweis.df " + "where word = '" + word + "';");
		session.close();
		for (Row row : results) {
			String number = row.toString().split(",")[1].split("]")[0];
			ret = Integer.parseInt(number.substring(1, number.length()));
			System.out.println(ret);
		}
		return ret;
	}

	public Map<String, String> readDFFromDatabase() {
		Map<String, String> map = new HashMap<>();
		Session session = cluster.connect();
		ResultSet results = session.execute("SELECT * FROM jweis.df;");
		session.close();
		for (Row row : results) {
			String word = row.toString().split(",")[0];
			String number = row.toString().split(",")[1].split("]")[0];
			map.put(word.substring(4, word.length()), number);
		}

		// Sortiert die Map
		map = map.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

		return map;
	}

	public int readTFFromDatabase(String word) {
		int ret = 0;
		Session session = cluster.connect();
		ResultSet results = session.execute("SELECT * FROM jweis.tf " + "where word = '" + word + "';");
		session.close();
		for (Row row : results) {
			String number = row.toString().split(",")[1].split("]")[0];
			ret = Integer.parseInt(number.substring(1, number.length()));
			System.out.println(ret);
		}
		return ret;
	}

	public Map<String, String> readTFFromDatabase() {
		Map<String, String> map = new HashMap<>();
		Session session = cluster.connect();
		ResultSet results = session.execute("SELECT * FROM jweis.tf;");
		session.close();
		for (Row row : results) {
			String word = row.toString().split(",")[0];
			String number = row.toString().split(",")[1].split("]")[0];
			map.put(word.substring(4, word.length()), number);
		}

		// Sortiert die Map
		map = map.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

		return map;
	}

}
