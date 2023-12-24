FROM openjdk:17-jdk-slim

COPY target/lib /lib/

COPY target/lib/vtl-spark-*.jar /vtl-spark.jar
COPY target/lib/vtl-model-*.jar /vtl-model.jar
COPY target/lib/vtl-engine-*.jar /vtl-engine.jar
COPY target/lib/vtl-parser-*.jar /vtl-parser.jar
COPY target/lib/vtl-jackson-*.jar /vtl-jackson.jar

COPY target/trevas-batch*.jar.original /lib/trevas-batch.jar

ENTRYPOINT ["java", "-cp", "/lib/*", "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED", "info.makingsense.trevas.batch.TrevasBatch"]