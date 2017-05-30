// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

// Project name
name := """multiplicacionMatrices"""

// Don't forget to set the version
version := "0.1.0-SNAPSHOT"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

// scala version to be used
scalaVersion := "2.11.8"
// force scalaVersion
//ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

// spark version to be used
val sparkVersion = "2.1.0"

// Needed as SBT's classloader doesn't work well with Spark
fork := true

// BUG: unfortunately, it's not supported right now
fork in console := true

// Java version
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

// add a JVM option to use when forking a JVM for 'run'
javaOptions ++= Seq("-Xmx2G")

// append -deprecation to the options passed to the Scala compiler
scalacOptions ++= Seq("-deprecation", "-unchecked")

// Use local repositories if they are needed.
// Please be careful for URIs if you are a Windows user.
// In case of Windows users, you should use "file:///" (Triple slash) URL expressions
// instead of "file://" (Double slash)
//
// resolvers ++= Seq(
//   Resolver.defaultLocal,
//   Resolver.mavenLocal,
//   // make sure default maven local repository is added... Resolver.mavenLocal has bugs.
//   "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
//   // For Typesafe goodies, if not available through maven
//   // "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
//   // For Spark development versions, if you don't want to build spark yourself
//   "Apache Staging" at "https://repository.apache.org/content/repositories/staging/"
//   )


/// Dependencies

// copy all dependencies into lib_managed/
//retrieveManaged := true

// scala modules (should be included by spark, just an exmaple)
//libraryDependencies ++= Seq(
//  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
//  "org.scala-lang" % "scala-compiler" % scalaVersion.value
//  )

val sparkDependencyScope = "provided"

// spark modules (should be included by spark-sql, just an example)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-sql" % sparkVersion % sparkDependencyScope
//  "org.apache.spark" %% "spark-mllib" % sparkVersion % sparkDependencyScope,
//  "org.apache.spark" %% "spark-streaming" % sparkVersion % sparkDependencyScope
)

// logging
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"

// testing
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.2" % "test"


/// Compiler plugins

addCompilerPlugin("org.psywerx.hairyfotr" %% "linter" % "0.1.12")


/// console

// define the statements initially evaluated when entering 'console', 'consoleQuick', or 'consoleProject'
// but still keep the console settings in the sbt-spark-package plugin

// If you want to use yarn-client for spark cluster mode, override the environment variable
// SPARK_MODE=yarn <cmd>
val sparkMode = sys.env.getOrElse("SPARK_MODE", "local[2]")

initialCommands in console :=
  s"""
    |import org.apache.spark.sql.SparkSession
    |
    |@transient val spark = SparkSession.builder().master("$sparkMode").appName("Console test").getOrCreate()
    |implicit def sc = spark.sparkContext
    |implicit def sqlContext = spark.sqlContext
    |import spark.implicits._
    |
    |def time[T](f: => T): T = {
    |  import System.{currentTimeMillis => now}
    |  val start = now
    |  try { f } finally { println("Elapsed: " + (now - start)/1000.0 + " s") }
    |}
    |
    |""".stripMargin

cleanupCommands in console :=
  s"""
     |spark.stop()
   """.stripMargin


/// scaladoc
scalacOptions in (Compile,doc) ++= Seq("-groups", "-implicits",
  // NOTE: remember to change the JVM path that works on your system.
  // Current setting should work for JDK7 on OSX and Linux (Ubuntu)
  "-doc-external-doc:/Library/Java/JavaVirtualMachines/jdk1.7.0_60.jdk/Contents/Home/jre/lib/rt.jar#http://docs.oracle.com/javase/7/docs/api",
  "-doc-external-doc:/usr/lib/jvm/java-7-openjdk-amd64/jre/lib/rt.jar#http://docs.oracle.com/javase/7/docs/api"
  )

autoAPIMappings := true

