ThisBuild / version := "1.0.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.6" //The Pekko documentation lists compatible Scala versions for each Pekko module.

lazy val abwcf = (project in file("."))
  .settings(
    name := "ABWCF"
  )

val pekkoVersion = "1.2.1" //License: Apache-2.0
val pekkoHttpVersion = "1.3.0" //License: Apache-2.0
val pekkoSlickVersion = "1.2.0" //License: Apache-2.0
val caffeineVersion = "3.2.3" //License: Apache-2.0
val crawlerCommonsVersion = "1.5" //License: Apache-2.0
val jsoupVersion = "1.21.2" //License: MIT
val openTelemetryApiVersion = "1.56.0" //License: Apache-2.0
val scalaTestVersion = "3.2.19" //License: Apache-2.0

libraryDependencies ++= Seq(
  "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
  "org.apache.pekko" %% "pekko-actor-testkit-typed" % pekkoVersion % Test,
  "org.apache.pekko" %% "pekko-cluster-typed" % pekkoVersion,
  "org.apache.pekko" %% "pekko-cluster-sharding-typed" % pekkoVersion,
  "org.apache.pekko" %% "pekko-connectors-slick" % pekkoSlickVersion,
  "org.apache.pekko" %% "pekko-stream" % pekkoVersion, //Required by Pekko HTTP and Pekko Connectors Slick.
  "org.apache.pekko" %% "pekko-http" % pekkoHttpVersion,
  "com.github.ben-manes.caffeine" % "caffeine" % caffeineVersion,
  "com.github.crawler-commons" % "crawler-commons" % crawlerCommonsVersion,
  "org.jsoup" % "jsoup" % jsoupVersion,
  "io.opentelemetry" % "opentelemetry-api" % openTelemetryApiVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)
