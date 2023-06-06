FROM openjdk:11-jre-slim

COPY target/lib /lib/

COPY target/lib/vtl-spark-*.jar /vtl-spark.jar
COPY target/lib/vtl-model-*.jar /vtl-model.jar
COPY target/lib/vtl-engine-*.jar /vtl-engine.jar
COPY target/lib/vtl-parser-*.jar /vtl-parser.jar

COPY target/trevas-batch*.jar.original /lib/trevas-batch.jar

ENTRYPOINT ["java", "-cp", "/lib/*", "info.makingsense.trevas.batch.TrevasBatch"]