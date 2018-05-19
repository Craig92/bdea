package database;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class MyCassandraCluster {

	private Cluster cluster;

	MyCassandraCluster() {
		cluster = Cluster.builder().addContactPoint("hoppy.informatik.hs-mannheim.de").withPort(9042).build();
	}

	public void writeDFIntoDatabase() {
		Session session = cluster.connect();
		session.execute("");
		session.close();
	}

	public void writeTFIntoDatabase() {
		Session session = cluster.connect();
		session.execute("");
		session.close();
	}

	public void readDFFromDatabase() {
		Session session = cluster.connect();
		ResultSet results = session.execute("");
		session.close();
	}

	public void readTFFromDatabase() {
		Session session = cluster.connect();
		ResultSet results = session.execute("");
		session.close();
	}

}
