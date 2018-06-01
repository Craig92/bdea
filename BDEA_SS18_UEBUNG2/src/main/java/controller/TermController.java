package controller;

import java.util.Map.Entry;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import database.MyCassandraCluster;

@RestController
public class TermController {
	
	private MyCassandraCluster cluster = new MyCassandraCluster();
	
	@GetMapping("/significallyTerm")
	public String getSignifikantTerm() {

		String result = "";
		for (Entry<String, String> entry :cluster.readDFFromDatabase().entrySet()) {
			result += "[" + entry.getKey() + " : " + entry.getValue() + "]\n";
		}
		return result;
	}

	@GetMapping("/mostFrequentTerm")
	public String getMostFrequentTerm() {

		String result = "";
		for (Entry<String, String> entry :cluster.readTFFromDatabase().entrySet()) {
			result += "[" + entry.getKey() + " : " + entry.getValue() + "]\n";
		}
		return result;
	}

}
