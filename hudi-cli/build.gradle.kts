/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.hudi.java-conventions")
}

dependencies {
    implementation("org.scala-lang:scala-library:2.11.12")
    implementation(project(":hudi-utilities-bundle_2.11"))
    implementation("org.apache.logging.log4j:log4j-1.2-api:2.17.2")
    implementation("org.springframework.shell:spring-shell:1.2.0.RELEASE")
    implementation("com.jakewharton.fliptables:fliptables:1.0.2")
    implementation("joda-time:joda-time:2.9.9")
    testImplementation(project(":hudi-common"))
    testImplementation(project(":hudi-client-common"))
    testImplementation(project(":hudi-spark-client"))
    testImplementation(project(":hudi-spark_2.11"))
    testImplementation(project(":hudi-utilities_2.11"))
    testImplementation("org.apache.logging.log4j:log4j-api:2.17.2")
    testImplementation("org.apache.logging.log4j:log4j-core:2.17.2")
    testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.17.2")
    testImplementation("org.apache.hadoop:hadoop-common:2.10.1")
    testImplementation("org.apache.hadoop:hadoop-hdfs:2.10.1")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.7.2")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.7.2")
    testImplementation("org.junit.vintage:junit-vintage-engine:5.7.2")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.7.2")
    testImplementation("org.mockito:mockito-junit-jupiter:3.3.3")
    testImplementation("org.junit.platform:junit-platform-runner:1.7.2")
    testImplementation("org.junit.platform:junit-platform-suite-api:1.7.2")
    testImplementation("org.junit.platform:junit-platform-commons:1.7.2")
    compileOnly("org.apache.hive:hive-jdbc:2.3.1")
    compileOnly("org.apache.parquet:parquet-avro:1.10.1")
    compileOnly("org.apache.spark:spark-core_2.11:2.4.4")
    compileOnly("org.apache.spark:spark-sql_2.11:2.4.4")
    compileOnly("org.apache.hadoop:hadoop-common:2.10.1")
    compileOnly("org.apache.hadoop:hadoop-hdfs:2.10.1")
}

description = "hudi-cli"
