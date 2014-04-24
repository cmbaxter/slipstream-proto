name := "slipstream-proto"

organization := "com.aquto"

version := Option(System.getProperty("slipstream.version")).getOrElse("0.0.0-SNAPSHOT")

scalaVersion := "2.10.0"

parallelExecution in Test := false

resolvers ++= Seq(
  "Scala Tools Repo Releases" at "http://scala-tools.org/repo-releases"
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.2",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.2" % "test"
)