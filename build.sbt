ThisBuild / version := "1.0.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.5"

lazy val root = (project in file("."))
  .settings(
    name := "Actor-Based Web Crawling Framework"
  )

val pekkoVersion = "1.1.3"
val scalaTestVersion = "3.2.19"

libraryDependencies ++= Seq(
  "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
  "org.apache.pekko" %% "pekko-actor-testkit-typed" % pekkoVersion % Test,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)
