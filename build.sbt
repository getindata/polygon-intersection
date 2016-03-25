name := "polygon-intersection"

fork := true

parallelExecution in Test := false

version := "1.0"

scalaVersion := "2.10.5"

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.5.1" % "provided",
  "org.apache.spark" %% "spark-hive" % "1.5.1" % "provided",
  "com.databricks" %% "spark-csv" % "1.4.0",
  "me.simin" %% "spatial-spark" % "1.1.0-SNAPSHOT",
  "org.slf4j" % "slf4j-log4j12" % "1.7.13" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList("org", "apache", xs@_*) => MergeStrategy.first
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
