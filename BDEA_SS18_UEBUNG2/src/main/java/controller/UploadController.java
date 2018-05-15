package controller;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import layer_batch.BatchJob;

@RestController
public class UploadController {

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
				File destination = new File(BatchJob.destinationPath + "file" + new Date().getTime());
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
