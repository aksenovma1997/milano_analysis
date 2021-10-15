name := "milan_task"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

resolvers += "Apache Software Foundation Snapshots" at "https://repository.apache.org/content/groups/snapshots"
//resolvers += "Geotools Wrapper for Apache Sedona" at "https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper"
//resolvers += "Jackson Databind" at "https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-databind"
//resolvers += "JTS" at "https://mvnrepository.com/artifact/org.locationtech.jts/jts-core"

libraryDependencies += "org.apache.sedona" %% "sedona-core-2.4" % "1.0.1-incubating"
libraryDependencies += "org.apache.sedona" %% "sedona-sql-2.4" % "1.0.1-incubating"
libraryDependencies += "org.apache.sedona" %% "sedona-viz-2.4" % "1.0.1-incubating"
libraryDependencies += "org.datasyslab" % "geotools-wrapper" % "1.1.0-24.1"
libraryDependencies += "org.apache.sedona" %% "sedona-python-adapter-2.4" % "1.0.1-incubating"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.0"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.0"

libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.18.2"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}