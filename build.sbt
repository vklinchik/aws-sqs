name := """aws-sqs"""

version := "0.1"
organization := "mindriot"

scalaVersion := "2.11.7"

val akkaVersion = "2.4.2"
val akkaStreamVersion = "2.0.3"
val logVersion = "1.1.3"
val specs2Version = "3.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %%  "akka-actor"                       % akkaVersion,
  "com.typesafe.akka" %%  "akka-testkit"                     % akkaVersion    % "test",
  "com.typesafe.akka" %   "akka-stream-experimental_2.11"     % akkaStreamVersion,
  //"com.typesafe.akka" % "akka-http-core-experimental_2.11"  % akkaStreamVersion,
  //"com.typesafe.akka" % "akka-http-experimental_2.11"       % akkaStreamVersion,
  "joda-time"         %   "joda-time"                        % "2.8.2",
  "org.specs2"        %%  "specs2-core"                      % specs2Version   % "test",
  "org.specs2"        %%  "specs2-mock"                      % specs2Version   % "test",
  "org.scalatest"     %%  "scalatest"                        % "2.2.4"         % "test",
  "com.amazonaws"     %   "aws-java-sdk"                     % "1.10.54",
  "ch.qos.logback"    %   "logback-core"                     % logVersion,
  "ch.qos.logback"    %   "logback-classic"                  % logVersion
)

scalacOptions in Test ++= Seq("-Yrangepos")




