name := "Interactive Audience Analytics"

version := "0.0.1"

organization := "com.collective.analytics"

scalaVersion := "2.10.5"

scalacOptions += "-deprecation"

scalacOptions += "-feature"

licenses in ThisBuild += ("MIT", url("http://opensource.org/licenses/MIT"))

fork in Test := true

// Resolvers

resolvers += "Cloudera"            at "https://repository.cloudera.com/artifactory/cloudera-repos/"

resolvers += "Collective Bintray"  at "http://dl.bintray.com/collectivemedia/releases"

// Library Dependencies

libraryDependencies ++= Seq(
  "org.apache.spark"         %% "spark-core"      % "1.4.0",
  "org.apache.spark"         %% "spark-sql"       % "1.4.0",
  "org.scala-tools.sbinary"  %% "sbinary"         % "0.4.2",
  "com.adroll.cantor"         % "cantor"          % "1.0.6"
)

// Test Dependencies

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest"   % "2.2.3" % "test"
)
