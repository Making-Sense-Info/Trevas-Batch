package info.makingsense.trevas.batch.utils;

import fr.insee.vtl.spark.SparkDataset;
import info.makingsense.trevas.batch.model.Input;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.nio.file.Path;
import java.util.Map;

public class SparkUtils {

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
                if (entry.getKey().startsWith("spark.")) {
                    conf.set(entry.getKey(), entry.getValue());
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

    public static SparkDataset readDataset(SparkSession spark, Input input) throws Exception {
        Dataset<Row> dataset;
        String location = input.getLocation();
        String format = input.getFormat();
        try {
            if ("csv".equals(format))
                dataset = spark.read()
                        .option("delimiter", ";")
                        .option("header", "true")
                        .csv(location);
            else if ("parquet".equals(format)) dataset = spark.read().parquet(location);
            else if ("sas".equals(format)) dataset = spark.read()
                    .format("com.github.saurfang.sas.spark")
                    .load(location);
            else if ("jdbc".equals(format)) dataset = readJDBCDataset(spark, input);
            else throw new Exception("Unknow S3 file type: " + format);
        } catch (Exception e) {
            throw new Exception("An error has occured while loading: " + location);
        }
        return new SparkDataset(dataset);
    }

    private static String getJDBCPrefix(String dbType) throws Exception {
        if (dbType.equals("postgre")) return "jdbc:postgresql://";
        if (dbType.equals("mariadb")) return "jdbc:mysql://";
        throw new Exception("Unsupported dbtype: " + dbType);
    }

    private static Dataset<Row> readJDBCDataset(SparkSession spark, Input input) throws Exception {
        String jdbcPrefix = "";
        String dbType = input.getDbType();
        String location = input.getLocation();
        String user = input.getUser();
        String password = input.getPassword();
        String query = input.getQuery();
        try {
            jdbcPrefix = getJDBCPrefix(dbType);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e);
        }
        DataFrameReader dfReader = spark.read().format("jdbc")
                .option("url", jdbcPrefix + location)
                .option("user", user)
                .option("password", password)
                .option("query", query);
        if (dbType.equals("postgre")) {
            dfReader.option("driver", "net.postgis.jdbc.DriverWrapper")
                    .option("driver", "org.postgresql.Driver");
        }
        if (dbType.equals("mariadb")) {
            dfReader.option("driver", "com.mysql.cj.jdbc.Driver");
        }
        return dfReader.load();
    }
}
