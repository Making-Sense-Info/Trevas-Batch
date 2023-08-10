package info.makingsense.trevas.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insee.vtl.spark.SparkDataset;
import info.makingsense.trevas.batch.model.BatchConfiguration;
import info.makingsense.trevas.batch.model.Input;
import info.makingsense.trevas.batch.model.Output;
import info.makingsense.trevas.batch.utils.SparkUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.immutable.Map;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.SimpleBindings;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static info.makingsense.trevas.batch.utils.NumberUtils.formatMs;
import static info.makingsense.trevas.batch.utils.SparkUtils.readDataset;
import static info.makingsense.trevas.batch.utils.SparkUtils.writeSparkDataset;
import static info.makingsense.trevas.batch.utils.TimeUtils.getDateNow;
import static info.makingsense.trevas.batch.utils.TimeUtils.getDateNowAsString;
import static java.time.temporal.ChronoUnit.MILLIS;

public class Engine {

    private static final Logger logger = LogManager.getLogger(Engine.class);

    public static void executeSpark(String configPath, String reportPath) throws Exception {
        if (configPath != null && !configPath.equals("")) {
            StringBuilder sb = new StringBuilder();
            sb.append("# Trevas Batch: " + getDateNowAsString() + "\n\n");

            StringBuilder sparkSessionStringBuilder = new StringBuilder();
            LocalDateTime beforeSparkSession = LocalDateTime.now();
            SparkSession spark = buildSparkSession();
            LocalDateTime afterSparkSession = LocalDateTime.now();
            long sparkSessionMs = MILLIS.between(beforeSparkSession, afterSparkSession);
            sparkSessionStringBuilder.append("### Spark session building\n\n");
            String sessionOpened = "Spark session was opened in " + formatMs(sparkSessionMs) + " milliseconds\n\n";
            sparkSessionStringBuilder.append(sessionOpened);
            logger.info(sessionOpened);

            Dataset<Row> json = spark.read().text(configPath);

            String collect = json.collectAsList().stream()
                    .map(r -> (String) r.get(0))
                    .collect(Collectors.joining(" "));

            ObjectMapper mapper = new ObjectMapper();
            BatchConfiguration batchConfiguration = mapper.readValue(collect, BatchConfiguration.class);
            List<Input> inputs = batchConfiguration.getInputs();
            List<Output> outputs = batchConfiguration.getOutputs();
            String script = batchConfiguration.getScript();

            sb.append("## Batch configuration\n\n");
            sb.append("Configuration file fetched from: " + configPath + "\n\n");
            sb.append("- Inputs: \n");
            inputs.forEach(i -> {
                sb.append("    - " + i.getName() + " (" + i.getFormat() + "): " + i.getLocation() + "\n");
            });
            sb.append("- Outputs: \n");
            outputs.forEach(i -> {
                sb.append("    - " + i.getName() + ": " + i.getLocation() + "\n");
            });
            sb.append("- Script: \n");
            sb.append("```\n");
            sb.append(script + "\n");
            sb.append("```\n\n");

            sb.append("## Spark configuration\n\n");
            Map<String, String> sparkConf = spark.conf().getAll();
            String sparkMaster = sparkConf.get("spark.master").isEmpty() ? "" : sparkConf.get("spark.master").get();
            sb.append("- spark.master: " + sparkMaster + "\n");
            String sparkDriverMemory = sparkConf.get("spark.driver.memory").isEmpty() ? "" : sparkConf.get("spark.driver.memory").get();
            sb.append("- spark.driver.memory: " + sparkDriverMemory + "\n");
            String sparkExecutorMemory = sparkConf.get("spark.executor.memory").isEmpty() ? "" : sparkConf.get("spark.executor.memory").get();
            sb.append("- spark.executor.memory: " + sparkExecutorMemory + "\n");
            String sparkExecutorRequestCores = sparkConf.get("spark.kubernetes.executor.request.cores").isEmpty() ? "" : sparkConf.get("spark.kubernetes.executor.request.cores").get();
            sb.append("- spark.kubernetes.executor.request.cores: " + sparkExecutorRequestCores + "\n");
            String sparkDynamicAllocation = sparkConf.get("spark.dynamicAllocation.enabled").isEmpty() ? "" : sparkConf.get("spark.dynamicAllocation.enabled").get();
            sb.append("- spark.dynamicAllocation.enabled: " + sparkDynamicAllocation + "\n");
            String sparkDynamicAllocationMinExec = sparkConf.get("spark.dynamicAllocation.minExecutors").isEmpty() ? "" : sparkConf.get("spark.dynamicAllocation.minExecutors").get();
            sb.append("- spark.dynamicAllocation.minExecutors: " + sparkDynamicAllocationMinExec + "\n");
            String sparkDynamicAllocationMaxExec = sparkConf.get("spark.dynamicAllocation.maxExecutors").isEmpty() ? "" : sparkConf.get("spark.dynamicAllocation.maxExecutors").get();
            sb.append("- spark.dynamicAllocation.maxExecutors: " + sparkDynamicAllocationMaxExec + "\n");
            sb.append("\n");

            sb.append("## Benchmarks\n\n");
            sb.append(sparkSessionStringBuilder);

            Bindings bindings = new SimpleBindings();
            ScriptEngine engine = SparkUtils.initEngineWithSpark(bindings, spark);

            LocalDateTime beforeRead = LocalDateTime.now();
            // Load datasets
            if (inputs != null) {
                sb.append("### Loading input datasets\n\n");
                inputs.forEach(input -> {
                    logger.info("Start to load inputs");
                    String name = input.getName();
                    try {
                        LocalDateTime beforeReadDs = LocalDateTime.now();
                        var ds = readDataset(spark, input);
                        LocalDateTime afterReadDs = LocalDateTime.now();
                        long readDs = MILLIS.between(beforeReadDs, afterReadDs);
                        bindings.put(name, ds);
                        // temp disable, not lazy?
                        int columns = ds.getDataStructure().size();
                        Dataset<Row> inputSparkDataset = ds.getSparkDataset();
                        Long rows = inputSparkDataset.count();
                        String dsRead = "- dataset `" + name + "` was read in " + formatMs(readDs) + " milliseconds (" + formatMs(columns) + " columns, " + formatMs(rows) + " rows)\n";
                        sb.append(dsRead);
                        logger.info(dsRead);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            sb.append("\n");
            LocalDateTime afterRead = LocalDateTime.now();
            long readTime = MILLIS.between(beforeRead, afterRead);
            logger.info("Inputs loaded in " + readTime + "ms");

            // Load script
            LocalDateTime beforeScript = LocalDateTime.now();
            long scriptTime = 0;
            if (script != null && !script.equals("")) {
                try {
                    engine.eval(script);
                } catch (Exception e) {
                    throw new Exception(e);
                }
                LocalDateTime afterScript = LocalDateTime.now();
                scriptTime = MILLIS.between(beforeScript, afterScript);
                sb.append("### VTL script execution\n\n");
                String scriptExecuted = "Script was executed in " + formatMs(scriptTime) + " milliseconds\n\n";
                sb.append(scriptExecuted);
                logger.info(scriptExecuted);
            }

            Bindings outputBindings = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);
            sb.append("### Execution plan\n\n");
            outputBindings.forEach((k, v) -> {
                if (v instanceof SparkDataset) {
                    sb.append("#### " + k + "\n\n");
                    Dataset<Row> sparkDs = ((SparkDataset) v).getSparkDataset();
                    String logicalPlan = sparkDs.queryExecution().logical().toString();
                    sb.append("- logical plan:\n" + logicalPlan + "\n");
                    String optimizedPlan = sparkDs.queryExecution().optimizedPlan().toString();
                    sb.append("- optimized plan:\n" + optimizedPlan + "\n");
                    String executedPlan = sparkDs.queryExecution().executedPlan().toString();
                    sb.append("- executed plan:\n" + executedPlan + "\n\n");
                }
            });

            LocalDateTime beforeWrite = LocalDateTime.now();
            // Load datasets to write
            if (outputs.size() > 0) {
                sb.append("### Writing output datasets\n\n");
                logger.info("Start to write outputs");
                outputs.forEach(output -> {
                    String name = output.getName();
                    String location = output.getLocation();
                    try {
                        LocalDateTime beforeWriteDs = LocalDateTime.now();
                        writeSparkDataset(location, (SparkDataset) outputBindings.get(name));
                        LocalDateTime afterWriteDs = LocalDateTime.now();
                        long writeDs = MILLIS.between(beforeWriteDs, afterWriteDs);
                        String outputWritten = "- dataset `" + name + "` was written in " + formatMs(writeDs) + " milliseconds\n";
                        sb.append(outputWritten);
                        logger.info(outputWritten);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                sb.append("\n");
            }
            LocalDateTime afterWrite = LocalDateTime.now();
            long writeTime = MILLIS.between(beforeWrite, afterWrite);
            logger.info("Outputs written in " + writeTime + "ms");

            sb.append("### Summary\n\n");
            long allMs = sparkSessionMs + readTime + scriptTime + writeTime;
            sb.append("|Task|Duration (ms)|Percentage (%)|\n");
            sb.append("|-|:-:|:-:|\n");
            sb.append("|Open Spark session|" + formatMs(sparkSessionMs) + "|" + sparkSessionMs * 100 / allMs + "|\n");
            sb.append("|Spark inputs loading (" + inputs.size() + " ds)|" + formatMs(readTime) + "|" + readTime * 100 / allMs + "|\n");
            sb.append("|VTL script execution|" + formatMs(scriptTime) + "|" + scriptTime * 100 / allMs + "|\n");
            sb.append("|Spark outputs writing (" + outputs.size() + " ds)|" + formatMs(writeTime) + "|" + writeTime * 100 / allMs + "|\n");
            sb.append("|**Total**|**" + formatMs(allMs) + "**|**100**|\n");

            // Write report
            if (reportPath != null && !reportPath.equals("")) {
                List<String> content = Arrays.asList(sb.toString().split("\n"));
                JavaSparkContext.fromSparkContext(spark.sparkContext())
                        .parallelize(content)
                        .coalesce(1)
                        .saveAsTextFile(reportPath + "-" + getDateNow());
            }
        }
    }

    private static SparkSession buildSparkSession() {
        SparkConf conf = SparkUtils.loadSparkConfig(System.getenv("SPARK_CONF_DIR"));
        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .appName("trevas-batch");
        if (!conf.get("spark.master").equals("local")) {
            conf.set("spark.jars", String.join(",",
                    "/vtl-spark.jar",
                    "/vtl-model.jar",
                    "/vtl-parser.jar",
                    "/vtl-engine.jar",
                    "/vtl-jackson.jar"
            ));
        }
        // Overwrite reports
        conf.set("spark.hadoop.validateOutputSpecs", "false");
        sparkBuilder.config(conf);
        return sparkBuilder.getOrCreate();
    }
}
