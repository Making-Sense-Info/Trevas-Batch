package info.makingsense.trevas.batch.utils;

import fr.insee.vtl.spark.SparkDataset;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.nio.file.Path;
import java.util.Map;

public class Utils {

    public static ScriptEngine initEngineWithSpark(Bindings bindings, SparkSession spark) {
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("vtl");
        ScriptContext context = engine.getContext();
        context.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
        engine.put("$vtl.engine.processing_engine_names", "spark");
        engine.put("$vtl.spark.session", spark);
        return engine;
    }

    public static SparkConf loadSparkConfig(String stringPath) {
        try {
            SparkConf conf = new SparkConf(true);
            if (stringPath != null) {
                Path path = Path.of(stringPath, "spark.conf");
                org.apache.spark.util.Utils.loadDefaultSparkProperties(conf, path.normalize().toAbsolutePath().toString());
            }

            for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
                var normalizedName = entry.getKey().toLowerCase().replace("_", ".");
                if (normalizedName.startsWith("spark.")) {
                    // TODO: find a better way to handle spark props
                    if (normalizedName.contains("dynamicallocation")) {
                        normalizedName = normalizedName.replace("dynamicallocation", "dynamicAllocation");
                    }
                    if (normalizedName.contains("shuffletracking")) {
                        normalizedName = normalizedName.replace("shuffletracking", "shuffleTracking");
                    }
                    if (normalizedName.contains("minexecutors")) {
                        normalizedName = normalizedName.replace("minexecutors", "minExecutors");
                    }
                    if (normalizedName.contains("maxexecutors")) {
                        normalizedName = normalizedName.replace("maxexecutors", "maxExecutors");
                    }
                    if (normalizedName.contains("extrajavaoptions")) {
                        normalizedName = normalizedName.replace("extrajavaoptions", "extraJavaOptions");
                    }
                    if (normalizedName.contains("pullpolicy")) {
                        normalizedName = normalizedName.replace("pullpolicy", "pullPolicy");
                    }
                    conf.set(normalizedName, entry.getValue());
                }
            }
            if (!conf.contains("spark.master")) {
                conf.set("spark.master", "local");
            }
            return conf;
        } catch (Exception ex) {
            throw ex;
        }
    }

    public static void writeSparkDataset(String path, SparkDataset dataset) {
        Dataset<Row> sparkDataset = dataset.getSparkDataset();
        sparkDataset.write()
                .mode(SaveMode.Overwrite)
                .parquet(path);
    }

    public static SparkDataset readDataset(SparkSession spark, String path, String fileType) throws Exception {
        Dataset<Row> dataset;
        try {
            if ("csv".equals(fileType))
                dataset = spark.read()
                        .option("delimiter", ";")
                        .option("header", "true")
                        .csv(path);
            else if ("parquet".equals(fileType)) dataset = spark.read().parquet(path);
            else if ("sas".equals(fileType)) dataset = spark.read()
                    .format("com.github.saurfang.sas.spark")
                    .load(path);
            else throw new Exception("Unknow S3 file type: " + fileType);
        } catch (Exception e) {
            throw new Exception("An error has occured while loading: " + path);
        }
        return new SparkDataset(dataset);
    }
}
