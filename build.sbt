ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.15"

lazy val root = (project in file(".")).settings(name := "batch-etl", idePackagePrefix := Some("it.mateusdesouza.spark"))

val sparkVersion = "3.5.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalactic" %% "scalactic" % "3.2.19",
  "org.scalatest" %% "scalatest" % "3.2.19" % "test",
  "com.typesafe" % "config" % "1.4.3",
  "org.scalatestplus" %% "mockito-5-12" % "3.2.19.0" % "test"
)
