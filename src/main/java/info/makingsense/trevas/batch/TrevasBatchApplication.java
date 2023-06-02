package info.makingsense.trevas.batch;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TrevasBatchApplication {

    @Value("${input.ds.path}")
    private String inputDSPath;

    @Value("${output.ds.path}")
    private String outputDSPath;

    @Value("${script.path}")
    private String scriptPath;

    public static void main(String[] args) {
        SpringApplication.run(TrevasBatchApplication.class, args);
    }

}
