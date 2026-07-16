// The Scala port of nightly_backfill.py. Same logic, same tests, so the two can be
// compared line for line — which is the point of M5.
//
// Scala 2.12, not 2.13 or 3, because that is the version Spark 3.5 ships its
// artifacts for. A Spark job compiled against a Scala version Spark was not built
// with fails at class-load time with a cryptic NoSuchMethodError, so the Scala
// version is dictated by Spark, not chosen.

ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "1.0.0"
ThisBuild / organization := "com.qcommerce"

val sparkVersion = "3.5.4"

lazy val root = (project in file("."))
  .settings(
    name := "nightly-backfill",

    libraryDependencies ++= Seq(
      // "provided": Spark is on the cluster/Dataproc classpath at runtime, so
      // bundling it into the assembly jar would produce a 200MB+ fat jar that
      // conflicts with the cluster's own Spark. provided means "compile against it,
      // do not ship it" — the standard scope for Spark dependencies.
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "io.delta" %% "delta-spark" % "3.2.1" % Provided,

      // Test scope: ScalaTest for the assertions, and Spark unprovided here so the
      // tests can spin up a local session (provided deps are absent at test time).
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.apache.spark" %% "spark-core" % sparkVersion % Test,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Test,
      "io.delta" %% "delta-spark" % "3.2.1" % Test
    ),

    // Spark needs these JVM options to run under Java 17 (the module system blocks
    // the reflective access Spark's serializers use). Without them the tests fail on
    // Java 17 with InaccessibleObjectException — the single most common "works on my
    // Java 8, fails in CI" Spark-on-Scala error.
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.calendar=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED"
    ),
    Test / fork := true,       // required for javaOptions to take effect
    Test / parallelExecution := false  // one SparkSession at a time
  )
