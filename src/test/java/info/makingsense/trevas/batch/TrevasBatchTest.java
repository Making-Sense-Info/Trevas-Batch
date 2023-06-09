package info.makingsense.trevas.batch;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static info.makingsense.trevas.batch.Engine.executeSpark;

@SpringBootTest
class TrevasBatchTest {

    @Test
    void contextLoads() throws Exception {
        StringBuilder sb = new StringBuilder();
        executeSpark(sb, "src/test/resources/input.csv",
                "src/test/resources/output.csv",
                "src/test/resources/script.txt",
                "src/test/resources/report");
    }

}
