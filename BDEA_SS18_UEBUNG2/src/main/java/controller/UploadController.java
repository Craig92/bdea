package controller;

import java.io.IOException;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
public class UploadController {

	@PostMapping("/upload")
	public String handleFileUplaod(@RequestParam("file") MultipartFile file) throws IOException {

		System.out.println(new String(file.getBytes()));
		return "";
	}

}
