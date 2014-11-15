name := "punch-forward"

organization := "com.github.overminder"

version := "0.1-SNAPSHOT"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-cluster" % "2.3.7",
  "com.github.nscala-time" %% "nscala-time" % "1.4.0"
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-testkit" % "2.3.7" % "test",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

