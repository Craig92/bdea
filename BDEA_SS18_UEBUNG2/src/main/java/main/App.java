package main;

import java.io.File;
import java.util.Timer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import layer_batch.BatchJob;

@SpringBootApplication
@ComponentScan(basePackages = "controller")
public class App {

	public final static String TOPIC = "jweis";
	public static final String SERVERS = "hoppy.informatik.hs-mannheim.de:9092";
	public static String destinationPath = new File("").getAbsolutePath() + "/src/main/resources/uploads";

	public static void main(String[] args) {

		SpringApplication.run(App.class, args);

		// Start in 5 Minuten, dann nach Ablauf alle 5 Minuten (300 Sekunden)
		new Timer().schedule(new BatchJob(), 300, 300000000);
	}
}
