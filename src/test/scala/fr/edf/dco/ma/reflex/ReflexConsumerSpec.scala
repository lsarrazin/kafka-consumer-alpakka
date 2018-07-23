package fr.edf.dco.ma.reflex

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.stream.ActorMaterializer
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.Config
import fr.edf.dco.ma.reflex.FilterActor.StopWorking
import fr.edf.dco.ma.reflex.ReflexProtocol.{ReflexEvent, ReflexMessage}
import fr.edf.dco.ma.reflex.embedded.EmbeddedKafkaBroker
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.Random

class ReflexConsumerSpec extends TestKit(ActorSystem("ReflexConsumerSpec"))
  with DefaultTimeout
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  /** **********************************************
    * Setup
    */

  //Initializing of ActorSystem, and various stuff needed for the ReflexConsumer
  val materializer: ActorMaterializer = ActorMaterializer.create(system)
  val ec: ExecutionContextExecutor = system.dispatcher

  val kafkaServer: EmbeddedKafkaBroker = new EmbeddedKafkaBroker(9092, 2181)

  // Create Kafka producer for test purpose
  val config: Config = system.settings.config.getConfig("akka.kafka.producer")
  val producerSettings: ProducerSettings[String, ReflexEvent] =
    ProducerSettings(config, new StringSerializer, new JsonSerializer[ReflexEvent])
      .withBootstrapServers("localhost:9092")
  val kafkaProducer: KafkaProducer[String, ReflexEvent] = producerSettings.createKafkaProducer()


  val bootstrapServers = "localhost:9092"
  val consumerGroup = "Reflex_SCG"
  val kafkaTopics = "topic1"
  val settings = new KafkaSettings(bootstrapServers, consumerGroup, kafkaTopics)


  /** **********************************************
    * Helper methods
    */

  override def beforeEach(): Unit = {
    kafkaServer.startup()
    kafkaServer.createTopic("topic1")
  }

  override def afterEach(): Unit = {
    kafkaServer.shutdown()
  }

  override def afterAll(): Unit = {
    kafkaProducer.close()
    shutdown(system)
  }

  def submitMsg(times: Int, topic: String, evt: ReflexEvent): Seq[ReflexMessage] = {
    def randomString: String = Random.alphanumeric.take(5).mkString("Id#", "", "")

    for (i <- 1 to times) yield {
      val sentMsg = kafkaProducer.send(new ProducerRecord(topic, randomString, evt))
      kafkaProducer.flush()
      new ReflexMessage(topic, sentMsg.get().partition(), sentMsg.get().offset(), sentMsg.get().timestamp(), randomString, evt)
    }
  }

  /** **********************************************
    * Tests
    */

  "A filter actor" must {

    //Setup of the actors
    val displayProbe = TestProbe()
    val rejectProbe = TestProbe()

    def filterFunction(r: ReflexMessage): Boolean = r.event.text.startsWith("Hello")

    val filterActor = system.actorOf(FilterActor.props(filterFunction, displayProbe.ref, rejectProbe.ref), "FilterTest")

    val tester = TestProbe("ReflexConsumerSpec")
    tester.watch(filterActor)

    //Now that the ActorSystem is in place, we run the actual consumer.
    val controller = new BusinessController(filterActor, rejectProbe.ref)
    val reflexSource = new ReflexConsumer(settings, controller)(system, ec, materializer)
    val control = reflexSource.kafkaSource


    "must apply faithfully the filter function" in {
      //We produce some messages and keep them for checks
      val sentMsgsOK: Seq[ReflexMessage] = submitMsg(10, "topic1", ReflexEvent("Hello World"))
      val sentMsgsKO: Seq[ReflexMessage] = submitMsg(10, "topic1", ReflexEvent("Goodbye World"))

      //Necessary pause to let the consumer do its job.
      Thread.sleep(5000)

      //Now we can shut down the source then the Actors.
      reflexSource.terminateWhenDone(control.shutdown())
      filterActor ! StopWorking

      //Success conditions :
      displayProbe.receiveN(10, 10 seconds)
      rejectProbe.receiveN(10, 10 seconds)
      tester.expectTerminated(filterActor, 10 seconds)
    }
  }

}

