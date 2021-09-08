description = "Optimizing Kafka Streams Topologies running on Kubernetes"
plugins {
    java
    id("io.freefair.lombok") version "5.3.3.3"
    id("com.google.cloud.tools.jib") version "3.1.4"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.2.1"
}

group = "com.bakdata.kafka"

tasks.withType<Test> {
    maxParallelForks = 4
    useJUnitPlatform()
}

repositories {
    mavenCentral()
    maven(url = "https://packages.confluent.io/maven/")
}


configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

dependencies {
    val confluentVersion: String by project
    implementation(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)
    implementation(group = "info.picocli", name = "picocli", version = "4.6.1")
    implementation(group = "com.bakdata.kafka", name = "streams-bootstrap", version = "1.8.0")
    implementation(group = "com.bakdata.kafka", name = "error-handling", version = "1.0.0")
    implementation(group = "com.bakdata.seq2", name = "seq2", version = "1.0.0")

    val junitVersion: String by project
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.20.2")
    testImplementation(group = "log4j", name = "log4j", version = "1.2.17")
    testImplementation(group = "com.bakdata.fluent-kafka-streams-tests", name = "fluent-kafka-streams-tests-junit5", version = "2.3.1")
}
