package info.makingsense.trevas.batch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import static info.makingsense.trevas.batch.Engine.executeSpark;

@SpringBootApplication
public class TrevasBatch {

    private static final Logger logger = LogManager.getLogger(TrevasBatch.class);

    @Value("${input.ds.path}")
    private static String inputDSPath;

    @Value("${output.ds.path}")
    private static String outputDSPath;

    @Value("${script.path}")
    private static String scriptPath;

    @Value("${report.path}")
    private static String reportPath;

    public static void main(String[] args) throws Exception {
        logger.warn("Batch configuration:");
        logger.warn("- input path: " + inputDSPath);
        logger.warn("- output path: " + outputDSPath);
        logger.warn("- script path: " + scriptPath);
        logger.warn("- report path: " + reportPath);
        executeSpark(inputDSPath, outputDSPath, scriptPath, reportPath);
    }

}
