package controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TermController {

	@GetMapping("/significallyTerm")
	public String getSignifikantTerm() {

		// TODO
		return "Der signifikanteste Term lautet: ";
	}

	@GetMapping("/mostFrequentTerm")
	public String getMostFrequentTerm() {

		// TODO
		return "Der häufigste Term lautet: ";
	}

}
