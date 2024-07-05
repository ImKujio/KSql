plugins {
    kotlin("jvm") version "1.9.23"
    kotlin("plugin.serialization") version "1.9.23"
}

group = "me.kujio"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    implementation("net.sourceforge.jtds:jtds:1.3.1")
    implementation("org.jetbrains.kotlin:kotlin-reflect:1.9.23")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(17)
}