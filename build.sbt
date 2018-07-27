name := "kafka-alpakka-consumer-actor"

version := "1.0"

scalaVersion := "2.12.6"

lazy val akkaVersion = "2.5.13"
lazy val playVersion = "2.6.9"

libraryDependencies ++= Seq(
  //Akka
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",

  // Alpakka 0.22 (Kafka 1.+)
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.22",

  //JSon
  "com.typesafe.play" %% "play-json" % playVersion,

  //Logging, we are using logback as our main framework.
  //We'll use slf4j to handle all the bridging required by the various dependencies
  //and primarily Kafka server that wants to bind to log4j
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.25",

  //For additional embedded joy
  "org.apache.commons" % "commons-io" % "1.3.2",
  "org.apache.kafka" %% "kafka" % "1.0.1"
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("log4j", "log4j")
)