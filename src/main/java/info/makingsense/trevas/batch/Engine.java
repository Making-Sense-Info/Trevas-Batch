package info.makingsense.trevas.batch;

import fr.insee.vtl.spark.SparkDataset;
import info.makingsense.trevas.batch.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.SimpleBindings;
import java.util.Arrays;
import java.util.List;

import static info.makingsense.trevas.batch.utils.Time.getDateNow;
import static info.makingsense.trevas.batch.utils.Utils.readDataset;
import static info.makingsense.trevas.batch.utils.Utils.writeSparkDataset;

public class Engine {

    public static void executeSpark(String inputDSPath, String outputDSPath,
                                    String scriptPath, String reportPath) throws Exception {
        String sb = getDateNow() + "\n\n" +
                "test";
        SparkSession spark = buildSparkSession();
        Bindings bindings = new SimpleBindings();
        ScriptEngine engine = Utils.initEngineWithSpark(bindings, spark);

        // Load datasets
        if (inputDSPath != null) {
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
        if (scriptPath != null) {
            String script = spark.read().textFile(scriptPath).collectAsList()
                    .stream().reduce((acc, current) -> acc + " \n\r" + current).orElse("");
            try {
                engine.eval(script);
            } catch (Exception e) {
                throw new Exception(e);
            }
        }

        // Load datasets to write
        if (outputDSPath != null) {
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

        // Write report
        if (reportPath != null) {
            List<String> content = Arrays.asList(sb.split("\n"));
            JavaSparkContext.fromSparkContext(spark.sparkContext())
                    .parallelize(content)
                    .coalesce(1)
                    .saveAsTextFile(reportPath);
        }
    }

    private static SparkSession buildSparkSession() throws Exception {
        SparkConf conf = Utils.loadSparkConfig(System.getenv("SPARK_CONF_DIR"));
        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .appName("trevas-batch");
        conf.set("spark.jars", String.join(",",
                "/vtl-spark.jar",
                "/vtl-model.jar",
                "/vtl-parser.jar",
                "/vtl-engine.jar"
        ));
        // Overwrite reports
        conf.set("spark.hadoop.validateOutputSpecs", "false");
        sparkBuilder.config(conf);
        return sparkBuilder.getOrCreate();
    }

}
