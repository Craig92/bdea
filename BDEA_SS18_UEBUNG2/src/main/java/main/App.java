package main;

import java.util.Timer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import layer_batch.BatchJob;

@SpringBootApplication
@ComponentScan(basePackages = "controller")
public class App {
	public static void main(String[] args) {
		SpringApplication.run(App.class, args);

		Timer timer = new Timer();

		// Start in 5 Minuten
		timer.schedule(new BatchJob(), 300000);

		// Start in einer Sekunde dann Ablauf alle 5 Minuten (300 Sekunden)
		timer.schedule(new BatchJob(), 1000, 300000);
	}
}
