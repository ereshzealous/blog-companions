import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.io.OutputStream

plugins {
    java
    id("org.springframework.boot") version "3.3.13"
    id("io.spring.dependency-management") version "1.1.7"
    id("me.champeau.jmh") version "0.7.2"
}

group = "com.rtb"
version = "0.1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

val jmhVersion = "1.37"

// Docker Engine 29.0 raised its minimum API version to 1.44, which the docker-java client inside
// the Testcontainers release Spring Boot 3.3 manages does not speak. 1.21.4 is the first 1.x fix.
extra["testcontainers.version"] = "1.21.4"

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("com.github.ben-manes.caffeine:caffeine:3.1.8")
    implementation("com.zaxxer:HikariCP")
    implementation("io.micrometer:micrometer-core")
    runtimeOnly("io.micrometer:micrometer-registry-prometheus")
    runtimeOnly("org.postgresql:postgresql")

    compileOnly("org.projectlombok:lombok")
    annotationProcessor("org.projectlombok:lombok")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:kafka")
    testImplementation("org.openjdk.jmh:jmh-core:$jmhVersion")
    testAnnotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:$jmhVersion")
    testCompileOnly("org.projectlombok:lombok")
    testAnnotationProcessor("org.projectlombok:lombok")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// ScopedValue (JEP 446) and StructuredTaskScope (JEP 453) are preview APIs on JDK 21: every
// compile, test, exec and JMH fork must opt in, or the class files will not load.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.addAll(listOf("--enable-preview", "-parameters"))
    options.encoding = "UTF-8"
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    jvmArgs("--enable-preview", "-XX:+EnableDynamicAgentLoading")
    maxHeapSize = "2g"
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = false
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

tasks.withType<JavaExec>().configureEach {
    jvmArgs("--enable-preview")
}

// The benchmarks live under src/test/java (so `./gradlew test` compiles them on every build);
// the JMH plugin compiles the same package into its own source set and runs it in a forked JVM.
sourceSets {
    named("jmh") {
        java.setSrcDirs(listOf("src/test/java"))
        java.include("com/rtb/dsp/benchmarks/**")
    }
}

// Every invocation gets a fresh evidence directory. Reports never combine it with archived runs.
val benchmarkStamp = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
    .withZone(ZoneOffset.UTC).format(Instant.now()) + "-" + UUID.randomUUID().toString().take(8)
val jmhRunDir = file("benchmarks/runs/${benchmarkStamp}-jmh")
val surgeRunDir = file("benchmarks/runs/${benchmarkStamp}-surge")
val consoleTool = file("tools/benchmark_console.py").absolutePath
val verifyPipeline = providers.gradleProperty("jmhVerify").map { it.toBoolean() }.getOrElse(false)
val runCommand = "./gradlew " + gradle.startParameter.taskNames.joinToString(" ") +
    listOf("jmhInclude", "jmhVerify", "surgeRps", "surgeCapacity").mapNotNull { key ->
        providers.gradleProperty(key).orNull?.let { " -P$key=$it" }
    }.joinToString("")

jmh {
    jmhVersion = "1.37"
    includeTests = false
    jvmArgs = listOf("--enable-preview")
    profilers = listOf("gc")
    resultFormat = "JSON"
    resultsFile = file("${jmhRunDir}/results.json")
    humanOutputFile = file("${jmhRunDir}/human.txt")
    failOnError = true
    duplicateClassesStrategy = DuplicatesStrategy.EXCLUDE
    (project.findProperty("jmhInclude") as String?)?.let { includes = listOf(it) }
    if (verifyPipeline) {
        warmupIterations = 1
        iterations = 2
        warmup = "200ms"
        timeOnIteration = "200ms"
        fork = 1
    }
}

val prepareJmhRun by tasks.registering(Exec::class) {
    dependsOn("jmhJar")
    commandLine("python3", consoleTool, "begin", "--run-dir", jmhRunDir,
        "--kind", if (verifyPipeline) "JMH pipeline verification" else "JMH benchmark", "--command", runCommand)
}
val finishJmhRun by tasks.registering(Exec::class) {
    onlyIf { file("${jmhRunDir}/manifest.json").exists() }
    doFirst {
        commandLine(listOf("python3", consoleTool, "finish", "--run-dir", jmhRunDir.absolutePath) +
            if (tasks.named("jmh").get().state.failure != null) listOf("--failed") else emptyList())
    }
}
tasks.named("jmh") {
    dependsOn(prepareJmhRun)
    outputs.upToDateWhen { false }
    finalizedBy(finishJmhRun)
}

val prepareSurgeRun by tasks.registering(Exec::class) {
    dependsOn("jmhJar")
    commandLine("python3", consoleTool, "begin", "--run-dir", surgeRunDir,
        "--kind", "Surge simulation", "--command", runCommand)
}
var surgeOutput: OutputStream? = null
val surgeBenchmark by tasks.registering(JavaExec::class) {
    group = "benchmark"
    description = "Run the surge simulation, capture CSV/text, and publish to the live console."
    dependsOn(prepareSurgeRun)
    javaLauncher = javaToolchains.launcherFor { languageVersion = JavaLanguageVersion.of(21) }
    classpath(tasks.named<Jar>("jmhJar").flatMap { it.archiveFile })
    mainClass = "com.rtb.dsp.benchmarks.SurgeSimulation"
    args(providers.gradleProperty("surgeRps").getOrElse("10000"),
        providers.gradleProperty("surgeCapacity").getOrElse("256"), "${surgeRunDir}/surge-timeline.csv")
    doFirst {
        surgeOutput = file("${surgeRunDir}/surge-summary.txt").outputStream()
        standardOutput = surgeOutput!!
        errorOutput = surgeOutput!!
    }
}
val finishSurgeRun by tasks.registering(Exec::class) {
    onlyIf { file("${surgeRunDir}/manifest.json").exists() }
    doFirst {
        surgeOutput?.close()
        commandLine(listOf("python3", consoleTool, "finish", "--run-dir", surgeRunDir.absolutePath) +
            if (surgeBenchmark.get().state.failure != null) listOf("--failed") else emptyList())
    }
}
surgeBenchmark { finalizedBy(finishSurgeRun) }

// The bytecode generator loads the compiled benchmark classes to read their annotations; class
// files compiled with --enable-preview refuse to load in a JVM started without it.
tasks.named<me.champeau.jmh.JmhBytecodeGeneratorTask>("jmhRunBytecodeGenerator") {
    jvmArgs.add("--enable-preview")
}
