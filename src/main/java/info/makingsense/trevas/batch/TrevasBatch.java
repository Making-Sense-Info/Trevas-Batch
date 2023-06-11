package info.makingsense.trevas.batch;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import static info.makingsense.trevas.batch.Engine.executeSpark;

@SpringBootApplication
public class TrevasBatch implements CommandLineRunner {

    @Value("${config.path}")
    private String configPath;

    @Value("${report.path}")
    private String reportPath;

    public static void main(String[] args) {
        SpringApplication.run(TrevasBatch.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        executeSpark(configPath, reportPath);
    }
}
