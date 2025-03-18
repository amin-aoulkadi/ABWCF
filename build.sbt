ThisBuild / version := "1.0.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.4" //The Pekko documentation lists compatible Scala versions for each Pekko module.

lazy val abwcf = (project in file("."))
  .settings(
    name := "Actor-Based Web Crawling Framework"
  )

val pekkoVersion = "1.1.3" //License: Apache-2.0
val pekkoHttpVersion = "1.1.0" //License: Apache-2.0
val jsoupVersion = "1.19.1" //License: MIT
val scalaTestVersion = "3.2.19" //License: Apache-2.0

libraryDependencies ++= Seq(
  "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
  "org.apache.pekko" %% "pekko-actor-testkit-typed" % pekkoVersion % Test,
  "org.apache.pekko" %% "pekko-cluster-typed" % pekkoVersion,
  "org.apache.pekko" %% "pekko-cluster-sharding-typed" % pekkoVersion,
  "org.apache.pekko" %% "pekko-stream" % pekkoVersion,
  "org.apache.pekko" %% "pekko-http" % pekkoHttpVersion,
  "org.jsoup" % "jsoup" % jsoupVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)
