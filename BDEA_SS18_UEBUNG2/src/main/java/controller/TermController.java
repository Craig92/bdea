package controller;

import java.util.Map.Entry;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.datastax.driver.mapping.annotations.Param;

import database.MyCassandraCluster;

@RestController
public class TermController {

	private MyCassandraCluster cluster = new MyCassandraCluster();

	@GetMapping("/significallyTerm")
	public String getSignifikantTerm() {
		String result = "";
		for (Entry<String, String> entry : cluster.readDFFromDatabase().entrySet()) {
			result += "[" + entry.getKey() + " : " + entry.getValue() + "]\n";
		}
		return result;
	}

	@GetMapping("/mostFrequentTerm")
	public String getMostFrequentTerm() {
		String result = "";
		for (Entry<String, String> entry : cluster.readTFFromDatabase().entrySet()) {
			result += "[" + entry.getKey() + " : " + entry.getValue() + "]\n";
		}
		return result;
	}

	@GetMapping("/tfidf")
	public String tfidf(@RequestParam("DandT") String dAndT) {
		System.out.println(dAndT);
		String result = "";
		int tf = cluster.readTFFromDatabase(dAndT);
		int df = cluster.readDFFromDatabase(dAndT.split(" ")[1]);
		if(df == 0)
			df = 1;
		//TODO  N = Dokumentenanzahl
		double tfid = tf * Math.log(10 / df);
		return result + tfid;
	}

}
