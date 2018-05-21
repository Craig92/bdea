package database;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.sun.tools.javac.util.List;

public class MyCassandraCluster {

	private Cluster cluster;

	public MyCassandraCluster() {
		cluster = Cluster.builder().addContactPoint("hoppy.informatik.hs-mannheim.de").withPort(9042).build();
	}

	public void writeDFIntoDatabase(String word, int df) {
		Session session = cluster.connect();
		session.execute("INSERT INTO jweis.df (word,value) " 
				+ "VALUES('" + word + "'," + df + ");");
		
		session.close();
	}

	public void writeTFIntoDatabase(String word, int tf) {
		Session session = cluster.connect();
		session.execute("INSERT INTO jweis.tf (word,value) " 
				+ "VALUES('" + word + "'," + tf + ");");
		
		session.close();
	}

	public int readDFFromDatabase(String word) {
		int ret = 0;
		Session session = cluster.connect();
		ResultSet results = session.execute("SELECT * FROM jweis.df " 
				+ "where word = '" + word + "';");
		session.close();
		for (Row row : results) {
			String number = row.toString().split(",")[1].split("]")[0];
			ret = Integer.parseInt(number.substring(1, number.length()));
			System.out.println(ret);
		}
		return ret;
	}

	public int readTFFromDatabase(String word) {
		int ret = 0;
		Session session = cluster.connect();
		ResultSet results = session.execute("SELECT * FROM jweis.tf " 
				+ "where word = '" + word + "';");
		session.close();
		for (Row row : results) {
			String number = row.toString().split(",")[1].split("]")[0];
			ret = Integer.parseInt(number.substring(1, number.length()));
			System.out.println(ret);
		}
		return ret;
	}

	// TEST
//	 public static void main(String[] args) {
//	 MyCassandraCluster c = new MyCassandraCluster();
//	 c.writeDFIntoDatabase("123", 123);
//	 c.readDFFromDatabase("test");
//	 c.readDFFromDatabase("123");
//	 }

}
