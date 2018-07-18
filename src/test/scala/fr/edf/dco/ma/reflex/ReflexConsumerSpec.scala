package fr.edf.dco.ma.reflex

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.Config
import fr.edf.dco.ma.reflex.FilterActor.StopWorking
import fr.edf.dco.ma.reflex.ReflexProtocol.{ReflexEvent, ReflexMessage}
import fr.edf.dco.ma.reflex.embedded.EmbeddedKafkaBroker
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.Random

class ReflexConsumerSpec (_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {
  def this() = this(ActorSystem("ReflexConsumerSpec"))

  //Initializing of ActorSystem, and various stuff needed for the ReflexConsumer
  val aSys: ActorSystem = ActorSystem("ReflexSpec")
  val materializer: ActorMaterializer = ActorMaterializer.create(aSys)
  val ec: ExecutionContextExecutor = aSys.dispatcher

  val kafkaServer: EmbeddedKafkaBroker = new EmbeddedKafkaBroker(9092, 2181)

  // Create Kafka producer for test purpose
  val config: Config = system.settings.config.getConfig("akka.kafka.producer")
  val producerSettings: ProducerSettings[String, ReflexEvent] =
    ProducerSettings(config, new StringSerializer, new JsonSerializer[ReflexEvent])
      .withBootstrapServers("localhost:9092")
  val kafkaProducer: KafkaProducer[String, ReflexEvent] = producerSettings.createKafkaProducer()

  override def beforeAll(): Unit = {
    kafkaServer.startup()
    kafkaServer.createTopic("topic1")
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    kafkaProducer.close()
    kafkaServer.shutdown()
  }

  def submitMsg(times: Int, topic: String, evt: ReflexEvent): Unit = {
    def randomString: String = Random.alphanumeric.take(5).mkString("Id#", "", "")

    for (i <- 1 to times) {
      kafkaProducer.send(new ProducerRecord(topic, randomString, evt))
    }
    kafkaProducer.flush()
  }

  /** **********************************************
    * Les tests
    */

  //A very poor test at first, you have to check manually that 10 messages have indeed been displayed
  //by the DisplayActor.
  //TODO : Handle a state in the DisplayActor to actually perform a real check...
  "A display actor" must {
    "display all filtered messages" in {

      def filterFunction(r: ReflexMessage): Boolean = true

      val rejectActor = system.actorOf(RejectActor.props, "RejectTest")
      val displayActor = system.actorOf(DisplayActor.props, "DisplayTest")
      val filterActor = system.actorOf(FilterActor.props(filterFunction, displayActor, rejectActor), "FilterTest")

      val tester = TestProbe("ReflexConsumerSpec")
      tester.watch(filterActor)
      tester.watch(displayActor)
      tester.watch(rejectActor)

      //Now that the ActorSystem is in place, we run the actual consumer.
      val bootstrapServers = "localhost:9092"
      val consumerGroup = "Reflex_SCG"
      val kafkaTopics = "topic1"
      val controller = new BusinessController(filterActor, rejectActor)

      val settings = new KafkaSettings(bootstrapServers, consumerGroup, kafkaTopics)
      val reflexSource = new ReflexConsumer(settings, controller)(aSys, ec, materializer)

      val control = reflexSource.kafkaSource

      //We produce some messages
      submitMsg(10, "topic1", ReflexEvent("Hello World !"))

      Thread.sleep(10000)

      //We shut down the source then the Actors.
      reflexSource.terminateWhenDone(control.shutdown())
      filterActor ! StopWorking

      tester.expectTerminated(filterActor, 20 seconds)
    }
  }

}

