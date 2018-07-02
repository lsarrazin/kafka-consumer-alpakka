name := "kafka-alpakka-consumer-actor"

version := "1.0"

scalaVersion := "2.12.6"

lazy val akkaVersion = "2.5.13"
lazy val playVersion = "2.6.9"

libraryDependencies ++= Seq(
  //Akka
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",

  // Alpakka 0.22 (Kafka 1.+)
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.22",

  //JSon
  "com.typesafe.play" %% "play-json" % playVersion,

  "org.slf4j" % "log4j-over-slf4j" % "1.7.21" % "test",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)
