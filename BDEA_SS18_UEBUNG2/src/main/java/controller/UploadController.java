package controller;

import java.io.File;
import java.io.IOException;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
public class UploadController {

	private int index = 1;
	private String destinationPath = "src/main/resources/";

	/**
	 * Upload files and save the file in the resources directory
	 * 
	 * @param file
	 *            the uploaded file
	 * @return
	 */
	@PostMapping("/upload")
	public String handleFileUpload(@RequestParam("file") MultipartFile file) {

		if (!file.isEmpty()) {
			try {
				File destination = new File(destinationPath + "file" + index);
				file.transferTo(destination);
				return "Erfolgreich gespeichert!";
			} catch (IOException e) {
				e.printStackTrace();
				return "Fehler: Datei konnte nicht gespeichert werden!";
			}
		} else {
			return "Fehler: Leere oder keine Datei übergeben!";
		}

	}

}
