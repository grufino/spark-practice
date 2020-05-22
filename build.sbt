import sbt.Keys.libraryDependencies

name := "spark-practice"

version := "1.0"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "2.4.3" % Compile,
  "org.apache.spark" %% "spark-sql" % "2.4.3" % Compile
)