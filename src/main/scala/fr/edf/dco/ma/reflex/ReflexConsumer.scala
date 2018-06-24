package fr.edf.dco.ma.reflex

import akka.actor.Actor
import akka.event.LoggingAdapter
import cakesolutions.kafka.akka.{ConsumerRecords, KafkaConsumerActor}
import cakesolutions.kafka.akka.KafkaConsumerActor.Subscribe
import com.typesafe.config.Config
import fr.edf.dco.ma.reflex.ReflexProtocol.ReflexMessage
import org.apache.kafka.common.serialization.StringDeserializer

import scala.util.Random

trait KafkaConfig{
  def config:Config
  def log: LoggingAdapter
  def randomString(len: Int= 5): String = Random.alphanumeric.take(len).mkString("")
}

trait ReflexConsumer extends KafkaConfig{
  this: Actor =>

  //for pattern matching in our receive method
  val msgExtractor = ConsumerRecords.extractor[java.lang.String, ReflexMessage]

  val kafkaConsumerActor = context.actorOf(
    KafkaConsumerActor.props(config,new StringDeserializer(), new JsonDeserializer[ReflexMessage], self),
    "ReflexKafkaConsumerActor"
  )

  def subscribe(topics: List[String]) =
    kafkaConsumerActor ! Subscribe.AutoPartition(topics)

}
