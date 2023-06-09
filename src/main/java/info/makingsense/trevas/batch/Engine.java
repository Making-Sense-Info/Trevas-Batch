package info.makingsense.trevas.batch;

import fr.insee.vtl.spark.SparkDataset;
import info.makingsense.trevas.batch.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Configuration;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.SimpleBindings;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static info.makingsense.trevas.batch.utils.Time.getDateNow;
import static info.makingsense.trevas.batch.utils.Utils.readDataset;
import static info.makingsense.trevas.batch.utils.Utils.writeSparkDataset;
import static java.time.temporal.ChronoUnit.MILLIS;

@Configuration
public class Engine {

    public static void executeSpark(StringBuilder sb, String inputDSPath, String outputDSPath,
                                    String scriptPath, String reportPath) throws Exception {
        sb.append("## Benchmarks\n\n");
        LocalDateTime beforeSparkSession = LocalDateTime.now();
        SparkSession spark = buildSparkSession();
        LocalDateTime afterSparkSession = LocalDateTime.now();
        long sparkSessionMs = MILLIS.between(beforeSparkSession, afterSparkSession);
        sb.append("### Spark session building\n\n");
        sb.append("Session was opened in " + sparkSessionMs + " milliseconds\n\n");
        Bindings bindings = new SimpleBindings();
        ScriptEngine engine = Utils.initEngineWithSpark(bindings, spark);

        // Load datasets
        if (inputDSPath != null && !inputDSPath.equals("")) {
            sb.append("### Loading input datasets\n\n");
            sb.append("TODO\n\n");
            spark.read().csv(inputDSPath).collectAsList().forEach(f -> {
                String name = f.getString(0);
                String fileType = f.getString(1);
                String filePath = f.getString(2);
                try {
                    bindings.put(name, readDataset(spark, filePath, fileType));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        // Load script
        if (scriptPath != null && !scriptPath.equals("")) {
            String script = spark.read().textFile(scriptPath).collectAsList()
                    .stream().reduce((acc, current) -> acc + " \n\r" + current).orElse("");
            try {
                LocalDateTime beforeScript = LocalDateTime.now();
                engine.eval(script);
                LocalDateTime afterScript = LocalDateTime.now();
                long scriptMs = MILLIS.between(beforeScript, afterScript);
                sb.append("## Script execution\n\n");
                sb.append("Script was executed in " + scriptMs + " milliseconds\n\n");
            } catch (Exception e) {
                throw new Exception(e);
            }
        }

        // Load datasets to write
        if (outputDSPath != null && !outputDSPath.equals("")) {
            sb.append("### Writing output datasets\n\n");
            sb.append("TODO\n\n");
            Bindings outputBindings = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);
            Dataset<Row> outputDSCsv = spark.read().csv(outputDSPath);
            outputDSCsv.collectAsList().forEach(f -> {
                String name = f.getString(0);
                String filePath = f.getString(1);
                try {
                    writeSparkDataset(filePath, (SparkDataset) outputBindings.get(name));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        LocalDateTime afterAll = LocalDateTime.now();
        long allMs = MILLIS.between(beforeSparkSession, afterAll);
        sb.append("### Total time\n\n");
        sb.append("Overal time was " + allMs + " milliseconds\n\n");

        // Write report
        if (reportPath != null && !inputDSPath.equals("")) {
            List<String> content = Arrays.asList(sb.toString().split("\n"));
            JavaSparkContext.fromSparkContext(spark.sparkContext())
                    .parallelize(content)
                    .coalesce(1)
                    .saveAsTextFile(reportPath + "-" + getDateNow());
        }
    }

    private static SparkSession buildSparkSession() {
        SparkConf conf = Utils.loadSparkConfig(System.getenv("SPARK_CONF_DIR"));
        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .appName("trevas-batch");
        if (!conf.get("spark.master").equals("local")) {
            conf.set("spark.jars", String.join(",",
                    "/vtl-spark.jar",
                    "/vtl-model.jar",
                    "/vtl-parser.jar",
                    "/vtl-engine.jar"
            ));
        }
        // Overwrite reports
        conf.set("spark.hadoop.validateOutputSpecs", "false");
        sparkBuilder.config(conf);
        return sparkBuilder.getOrCreate();
    }

}
