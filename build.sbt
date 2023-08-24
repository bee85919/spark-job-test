ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "de-spark-example"
  )

val sparkVersion = "3.3.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

name              := "de-spark-example"

version           := "1.0"

organization      := "de.spark"

scalaVersion      := "2.12.15"

publishMavenStyle := true