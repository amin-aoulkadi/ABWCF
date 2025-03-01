ThisBuild / version := "1.0.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.5"

lazy val root = (project in file("."))
  .settings(
    name := "Actor-Based Web Crawling Framework"
  )
