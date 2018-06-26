package fr.edf.dco.ma.reflex

import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

import akka.kafka.scaladsl.Producer
import akka.kafka.ProducerSettings

import org.apache.kafka.common.serialization.{ ByteArraySerializer, StringSerializer }
import org.apache.kafka.clients.producer.ProducerRecord

/*
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
*
*/

import scala.concurrent.Future
import akka.Done

import scala.util.{ Failure, Success }

import FilterActor.StopWorking
import ReflexProtocol.ReflexMessage

class ReflexConsumerSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  implicit val system = ActorSystem("ReflexConsumer")
  implicit val ec = system.dispatcher

  // Create Kafka producer for test purpose
  val config = system.settings.config.getConfig("akka.kafka.producer")
  val producerSettings =
    ProducerSettings(config, new StringSerializer, new JsonSerializer[ReflexMessage])
      .withBootstrapServers("localhost:9092")
  val kafkaProducer = producerSettings.createKafkaProducer()

  /*


  def this() = this(ActorSystem("MySpec"))

  val kafkaServer = new KafkaServer()
  def randomString: String = Random.alphanumeric.take(5).mkString("")

  val config: Config = ConfigFactory.parseString(
    s"""
       | bootstrap.servers = "localhost:${kafkaServer.kafkaPort}",
       | auto.offset.reset = "earliest",
       | group.id = "$randomString"
        """.stripMargin)

  override def beforeAll() = {
    kafkaServer.startup()
  }

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
    producer.close()
    kafkaServer.close()
  }
  val keySerializer = new StringSerializer()
  val valueSerializer = new JsonSerializer[ReflexMessage]
  val keyDeserializer = new StringDeserializer()
  val valueDeserializer = new JsonDeserializer[ReflexMessage]

  val producer =
    KafkaProducer(KafkaProducer.Conf(keySerializer, valueSerializer, bootstrapServers = s"localhost:${kafkaServer.kafkaPort}"))

  def submitMsg(times: Int, topic: String, msg: ReflexMessage) =
    for (i <- 1 to times) {
      producer.send(KafkaProducerRecord(topic, randomString, msg))
      producer.flush()

  */

  def terminateWhenDone(result: Future[Done]): Unit =
    result.onComplete {
      case Failure(e) =>
        system.log.error(e, e.getMessage)
        system.terminate()
      case Success(_) => system.terminate()
    }

  def submitMsg(times: Int, topic: String, msg: ReflexMessage) = {

    def randomString: String = Random.alphanumeric.take(5).mkString("Id#", "", "")

    for (i <- 1 to times) {
      kafkaProducer.send(new ProducerRecord(topic, randomString, msg))
      kafkaProducer.flush()
    }
  }

/************************************************
   * Les tests
   */

  "A display actor" must {
    "display all filtered messages" in {

      def filterFunction(r: ReflexMessage): Boolean = true

      val displayActor = system.actorOf(DisplayActor.props, "DisplayTest")
      val filterActor = system.actorOf(FilterActor.props(filterFunction, displayActor), "FilterTest")
      val sourceActor = system.actorOf(ReflexConsumerActor.props(filterActor, "test", config), "KafkaConsumerTest")

      val tester = TestProbe("ReflexConsumerSpec")
      tester.watch(filterActor)
      tester.watch(displayActor)

      submitMsg(10, "test", ReflexMessage("Hello World !"))

      Thread.sleep(5000)

      filterActor ! StopWorking

      tester.expectTerminated(filterActor, 10 seconds)
    }
  }

}

