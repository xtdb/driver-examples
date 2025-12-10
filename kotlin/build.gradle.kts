plugins {
    kotlin("jvm") version "2.1.20"
    application
}

repositories {
    mavenCentral()
    maven { url = uri("https://repo.xtdb.com/maven-releases") }
    maven { url = uri("https://central.sonatype.com/repository/maven-snapshots/") }
}

dependencies {
    // XTDB API (includes XTDB JDBC driver)
    implementation("com.xtdb:xtdb-api:2.x-SNAPSHOT")

    // PostgreSQL JDBC Driver (fallback)
    implementation("org.postgresql:postgresql:42.7.1")

    // Transit-Java for transit-JSON support (using same library as Java)
    implementation("com.cognitect:transit-java:1.0.371")

    // JSON processing
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.1")

    // Testing
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.1")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.10.1")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // Arrow ADBC Flight SQL Driver (same versions as XTDB)
    testImplementation("org.apache.arrow.adbc:adbc-driver-flight-sql:0.20.0")
    testImplementation("org.apache.arrow:arrow-memory-netty:18.3.0")
}

application {
    mainClass.set("MainKt")
}

tasks.test {
    useJUnitPlatform()
    // Required for Apache Arrow memory access on Java 9+
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED")
}
