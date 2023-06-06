package info.makingsense.trevas.batch;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import static info.makingsense.trevas.batch.Engine.executeSpark;

@SpringBootApplication
public class TrevasBatch {

    @Value("${input.ds.path}")
    private static String inputDSPath;

    @Value("${output.ds.path}")
    private static String outputDSPath;

    @Value("${script.path}")
    private static String scriptPath;

    @Value("${report.path}")
    private static String reportPath;

    public static void main(String[] args) throws Exception {
        executeSpark(inputDSPath, outputDSPath, scriptPath, reportPath);
    }

}
