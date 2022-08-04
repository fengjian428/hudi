/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.hudi.java-conventions")
}

dependencies {
    implementation(project(":hudi-client-common"))
    implementation(project(":hudi-spark-client"))
    implementation(project(":hudi-common"))
    implementation(project(":hudi-hive-sync"))
    implementation(project(":hudi-spark-common_2.11"))
    implementation(project(":hudi-spark_2.11"))
    implementation(project(":hudi-spark2_2.11"))
    implementation(project(":hudi-spark2-common"))
    implementation("org.apache.kafka:kafka-clients:2.0.0")
    implementation("org.apache.pulsar:pulsar-client:2.8.1")
    implementation("org.apache.logging.log4j:log4j-api:2.17.2")
    implementation("org.apache.logging.log4j:log4j-1.2-api:2.17.2")
    implementation("org.slf4j:slf4j-api:1.7.30")
    implementation("com.fasterxml.jackson.module:jackson-module-scala_2.11:2.6.7.1")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-csv:2.6.7")
    implementation("org.apache.spark:spark-streaming_2.11:2.4.4")
    implementation("org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.4")
    implementation("org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.4")
    implementation("io.dropwizard.metrics:metrics-core:4.1.1")
    implementation("org.antlr:stringtemplate:4.0.2")
    implementation("com.beust:jcommander:1.72")
    implementation("com.twitter:bijection-avro_2.11:0.9.7")
    implementation("io.confluent:kafka-avro-serializer:5.3.4")
    implementation("io.confluent:common-config:5.3.4")
    implementation("io.confluent:common-utils:5.3.4")
    implementation("io.confluent:kafka-schema-registry-client:5.3.4")
    implementation("org.apache.httpcomponents:httpcore:4.4.1")
    implementation("com.amazonaws:aws-java-sdk-sqs:1.12.22")
    testImplementation("com.h2database:h2:1.4.199")
    testImplementation("org.eclipse.jetty.aggregate:jetty-all:9.4.15.v20190215")
    testImplementation("org.apache.logging.log4j:log4j-core:2.17.2")
    testImplementation("org.apache.kafka:kafka_2.11:2.0.0")
    testImplementation("org.apache.hadoop:hadoop-hdfs:2.10.1")
    testImplementation("org.apache.hadoop:hadoop-common:2.10.1")
    testImplementation(project(":hudi-client-common"))
    testImplementation(project(":hudi-spark-client"))
    testImplementation(project(":hudi-common"))
    testImplementation(project(":hudi-hive-sync"))
    testImplementation(project(":hudi-spark_2.11"))
    testImplementation("org.apache.hive:hive-exec:2.3.1")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.7.2")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.7.2")
    testImplementation("org.junit.vintage:junit-vintage-engine:5.7.2")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.7.2")
    testImplementation("org.mockito:mockito-junit-jupiter:3.3.3")
    testImplementation("org.junit.platform:junit-platform-runner:1.7.2")
    testImplementation("org.junit.platform:junit-platform-suite-api:1.7.2")
    testImplementation("org.junit.platform:junit-platform-commons:1.7.2")
    compileOnly("org.apache.parquet:parquet-avro:1.10.1")
    compileOnly("org.apache.spark:spark-core_2.11:2.4.4")
    compileOnly("org.apache.spark:spark-sql_2.11:2.4.4")
    compileOnly("org.apache.hadoop:hadoop-client:2.10.1")
    compileOnly("org.apache.hadoop:hadoop-mapreduce-client-common:2.10.1")
    compileOnly("org.apache.hive:hive-jdbc:2.3.1")
    compileOnly("org.apache.hive:hive-service:2.3.1")
}

description = "hudi-utilities_2.11"

val testsJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets["test"].output)
}

(publishing.publications["maven"] as MavenPublication).artifact(testsJar)
