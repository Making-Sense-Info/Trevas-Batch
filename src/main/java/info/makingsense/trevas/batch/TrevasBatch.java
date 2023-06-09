package info.makingsense.trevas.batch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import static info.makingsense.trevas.batch.Engine.executeSpark;
import static info.makingsense.trevas.batch.utils.Time.getDateNowAsString;

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
        StringBuilder sb = new StringBuilder();
        sb.append("# Trevas Batch: " + getDateNowAsString() + "\n\n");
        logger.error("Batch configuration:");
        sb.append("## Batch configuration\n\n");
        logger.warn("- input path: " + inputDSPath);
        sb.append("- input path: " + inputDSPath);
        logger.info("- output path: " + outputDSPath);
        sb.append("- output path: " + outputDSPath);
        logger.warn("- script path: " + scriptPath);
        sb.append("- script path: " + scriptPath);
        logger.warn("- report path: " + reportPath);
        sb.append("- report path: " + reportPath);
        sb.append("\n\n");
        sb.append("## Spark configuration\n\n");
        sb.append("TODO\n\n");
        executeSpark(sb, inputDSPath, outputDSPath, scriptPath, reportPath);
    }
}
