FROM openjdk:11-jre-slim

COPY target/trevas-batch*.jar.original /lib/trevas-batch.jar

ENTRYPOINT ["java", "-cp", "/lib/*", "info.makingsense.trevas.batch.TrevasBatch"]