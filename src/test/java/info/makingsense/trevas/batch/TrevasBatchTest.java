package info.makingsense.trevas.batch;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static info.makingsense.trevas.batch.Engine.executeSpark;

@SpringBootTest
class TrevasBatchTest {

    @Test
    void fakeBatch() throws Exception {
        executeSpark("src/test/resources/input.json",
                "src/test/resources/report");
    }

}
