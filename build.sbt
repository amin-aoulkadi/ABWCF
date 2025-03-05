ThisBuild / version := "1.0.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.4" //The Pekko documentation lists compatible Scala versions for each Pekko module.

lazy val root = (project in file("."))
  .settings(
    name := "Actor-Based Web Crawling Framework"
  )

val pekkoVersion = "1.1.3" //License: Apache-2.0
val logbackVersion = "1.5.17" //License: EPL / LGPL (dual license) → Derivative work must also be licensed under one of these.
val scalaTestVersion = "3.2.19" //License: Apache-2.0

libraryDependencies ++= Seq(
  "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
  "org.apache.pekko" %% "pekko-actor-testkit-typed" % pekkoVersion % Test,
  "org.apache.pekko" %% "pekko-cluster-typed" % pekkoVersion,
  "org.apache.pekko" %% "pekko-cluster-sharding-typed" % pekkoVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion % Runtime, //This dependency should probably not be included in release packages.
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)
