name := """aws-sqs"""

version := "0.1"
organization := "mindriot"

scalaVersion := "2.11.7"

val akkaVersion = "2.3.14"
val logVersion = "1.1.3"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor"                    % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit"                  % akkaVersion    % "test",
  "com.typesafe.akka" %  "akka-stream-experimental_2.11" % "1.0",
  "joda-time"         %  "joda-time"                     % "2.8.2",
  "org.specs2"        %% "specs2-core"                   % "3.6.4"        % "test",
  "org.scalatest"     %% "scalatest"                     % "2.2.4"        % "test",
  "com.amazonaws"     %  "aws-java-sdk"                  % "1.10.0",
  "ch.qos.logback"    %  "logback-core"                  % logVersion,
  "ch.qos.logback"    %  "logback-classic"               % logVersion
)



