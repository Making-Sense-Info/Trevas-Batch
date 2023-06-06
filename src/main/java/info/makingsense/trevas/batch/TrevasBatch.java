package info.makingsense.trevas.batch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import static info.makingsense.trevas.batch.Engine.executeSpark;

@SpringBootApplication
public class TrevasBatch implements CommandLineRunner {

    private static final Logger logger = LogManager.getLogger();

    @Value("${input.ds.path}")
    private String inputDSPath;
    @Value("${output.ds.path}")
    private String outputDSPath;
    @Value("${script.path}")
    private String scriptPath;
    @Value("${report.path}")
    private String reportPath;

    public static void main(String[] args) throws Exception {
        SpringApplication.run(TrevasBatch.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        logger.error("Batch configuration:");
        logger.warn("- input path: " + inputDSPath);
        logger.info("- output path: " + outputDSPath);
        logger.warn("- script path: " + scriptPath);
        logger.warn("- report path: " + reportPath);
        executeSpark(inputDSPath, outputDSPath, scriptPath, reportPath);
    }
}
