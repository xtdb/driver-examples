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
}

application {
    mainClass.set("MainKt")
}

tasks.test {
    useJUnitPlatform()
}
