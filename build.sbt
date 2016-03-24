name := "polygon-intersection"

fork := true

parallelExecution in Test := false

version := "1.0"

scalaVersion := "2.10.5"

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.1",
  "org.apache.spark" %% "spark-core" % "1.5.1",
  "org.apache.spark" %% "spark-sql" % "1.5.1",
  "org.apache.spark" %% "spark-hive" % "1.5.1",
  "com.databricks" %% "spark-csv" % "1.4.0",
  "me.simin" %% "spatial-spark" % "1.1.0-SNAPSHOT",
  "log4j" % "log4j" % "1.2.15" exclude("javax.jms", "jms") exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri")
)

assemblyExcludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  cp filter { x => x.data.getName.matches("sbt.*") || x.data.getName.matches(".*macros.*") || x.data.getName.matches("spark.*") }
}
