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
	public String getMostFrequentTerm(@RequestParam("data") String tfName) {
		System.out.println(tfName);
		String result = "";
		int tf = cluster.readTFFromDatabase(tfName);
		int df = cluster.readDFFromDatabase(tfName.split(" ")[1]);
		if(df == 0)
			df = 1;
		double tfid = tf * Math.log(10 / df);
		return result + tfid;
	}

}
