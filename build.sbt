name := "spark-tutorial"

version := "0.0.1"

scalaVersion := "2.11.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"

scalaSource in Compile := baseDirectory.value / "src/main/scala"