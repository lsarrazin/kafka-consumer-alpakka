name := "kafka-consumer-actor"

version := "1.0"

scalaVersion := "2.11.12"

resolvers += Resolver.bintrayRepo("cakesolutions", "maven")

lazy val akkaVersion = "2.5.13"

libraryDependencies ++= Seq(
  //Akka
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",

  //scala-kafka-client
  "net.cakesolutions" %% "scala-kafka-client" % "0.10.0.0",
  "net.cakesolutions" %% "scala-kafka-client-akka" % "0.10.0.0",
  "net.cakesolutions" %% "scala-kafka-client-testkit" % "0.10.0.0",

  //JSon
  "com.typesafe.play" % "play-json_2.11" % "2.5.12",

  "org.slf4j" % "log4j-over-slf4j" % "1.7.21" % "test"
  //TODO: Décommenter pour des logs console verbeux (logger à implémenter correctement)
  //"org.slf4j" % "slf4j-simple" % "1.6.4"
)