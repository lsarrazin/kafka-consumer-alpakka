package fr.edf.dco.ma.reflex

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, PoisonPill, Props }

import akka.event.LoggingAdapter

import com.typesafe.config.Config

import akka.kafka.scaladsl.Consumer

import akka.kafka.ConsumerMessage.{ CommittableMessage, CommittableOffsetBatch }
import akka.kafka.ConsumerSettings
import akka.kafka.Subscriptions

import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import akka.stream.ActorMaterializer

import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, StringDeserializer }
import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerRecord }

import ReflexProtocol.ReflexMessage

import akka.Done

import scala.util.{ Failure, Success }

import scala.concurrent.Future

/*
trait KafkaConfig {
  def config:Config
  def log: LoggingAdapter
  def randomString(len: Int= 5): String = Random.alphanumeric.take(len).mkString("")
}

trait ReflexConsumer extends KafkaConfig {
  this: Actor =>

  //for pattern matching in our receive method
  val msgExtractor = ConsumerRecords.extractor[java.lang.String, ReflexMessage]

  val kafkaConsumerActor = context.actorOf(
    KafkaConsumerActor.props(config,new StringDeserializer(), new JsonDeserializer[ReflexMessage], self),
    "ReflexKafkaConsumerActor"
  )

  def subscribe(topics: List[String]) =
    kafkaConsumerActor ! Subscribe.AutoPartition(topics)

}*/

// Consume messages at-least-once
class ALOConsumer(topic: String, config: Config) {

  val system = ActorSystem("ReflexConsumer")
  implicit val ec = system.dispatcher
  implicit val m = ActorMaterializer.create(system)

  val maxPartitions = 100

  val _config = config.getConfig("akka.kafka.consumer")
  val consumerSettings =
    ConsumerSettings(_config, new StringDeserializer, new JsonDeserializer[ReflexMessage])
      .withBootstrapServers("localhost:9092")
      .withGroupId("group1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val consumerSettingsWithAutoCommit =
    consumerSettings
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")

  def business[T] = Flow[T]
  def businessLogic(record: ConsumerRecord[String, Array[Byte]]): Future[Done] = ???

  def terminateWhenDone(result: Future[Done]): Unit =
    result.onComplete {
      case Failure(e) =>
        system.log.error(e, e.getMessage)
        system.terminate()
      case Success(_) => system.terminate()
    }

  def apply(filter: ActorRef): Unit = {

    def business(key: String, value: ReflexMessage): Future[Done] = {
      filter ! value
      Done
    }

    val control =
      Consumer
        .committableSource(consumerSettings, Subscriptions.topics("topic1"))
        .mapAsync(10) { msg =>
          business(msg.record.key, msg.record.value).map(_ => msg.committableOffset)
        }
        .mapAsync(5)(offset => offset.commitScaladsl())
        .toMat(Sink.ignore)(Keep.both)
        .run()
  }

}