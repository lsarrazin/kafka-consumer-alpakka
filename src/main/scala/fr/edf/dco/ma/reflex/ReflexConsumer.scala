package fr.edf.dco.ma.reflex

import akka.{ Done, NotUsed }
import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, Props }

import akka.kafka._
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.{ ConsumerSettings, ProducerSettings, Subscriptions }
import akka.kafka.ConsumerMessage.{ CommittableMessage, CommittableOffsetBatch }

import akka.stream.scaladsl.{ Flow, Keep, RestartSource, Sink, Source }
import akka.stream.ActorMaterializer

import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerRecord }
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.{ Metric, MetricName, TopicPartition }
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer }

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import java.util.concurrent.atomic.AtomicLong

import ReflexProtocol.{ ReflexMessage, SpuriousMessage }
import ReflexProtocol.ReflexEvent


class KafkaSettings(bootstrapServers: String, consumerGroup: String, val topics: String)(implicit system: ActorSystem) {

  lazy val configC = system.settings.config.getConfig("akka.kafka.consumer")
  def consumerSettings =
    ConsumerSettings(configC, new ByteArrayDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId(consumerGroup)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

}

class BusinessController(procActor: ActorRef, errActor: ActorRef) {

  type Service[A, C] = A => Future[C]
  
  val reflexDeserializer = new JsonDeserializer[ReflexEvent]

  val handleMessage: Service[CommittableMessage[Array[Byte], Array[Byte]], Done] =
    (msg) => {
      // Convert key/value pair to expected types
      val key = try {
        new String(msg.record.key)
      } catch {
        case _: Throwable => "!NoKey!"
      }
      try {
        val value = reflexDeserializer.deserialize(msg.record.topic, msg.record.value)
        procActor ! ReflexMessage(msg.record.topic, msg.record.partition, msg.record.offset, msg.record.timestamp, key, value)
        Future.successful(Done)
      } catch {
        case e: Throwable => println("Deserialization error: " + e.getMessage)
        errActor ! SpuriousMessage(msg.record.topic, msg.record.partition, msg.record.offset, msg.record.timestamp, key, new String(msg.record.value)) 
        Future.successful(Done)
      }
    }

}

class ReflexConsumer(settings: KafkaSettings, bc: BusinessController)(implicit system: ActorSystem, context: ExecutionContextExecutor) {

  type KafkaMessage = CommittableMessage[Array[Byte], Array[Byte]]

  // Create consumer settings
  val consumerSettings = settings.consumerSettings
  val kafkaTopics = settings.topics

  // Create source connector from kafka settings, and apply business logic
  def kafkaSource =
    Consumer
      .committableSource(consumerSettings, Subscriptions.topics(kafkaTopics))
      .mapAsync(1) { msg =>
        println("Received: " + msg.record.key + " => " + msg.record.value)
        bc.handleMessage(msg)
          .flatMap {
            response =>
              msg.committableOffset.commitScaladsl
          } 
          .recoverWith{
            case e =>
              system.log.error("An exception occured: ", e)
              msg.committableOffset.commitScaladsl
          }
      }

  def terminateWhenDone(result: Future[Done]): Unit =
    result.onComplete {
      case Failure(e) =>
        system.log.error(e, e.getMessage)
        system.terminate()
      case Success(_) => system.terminate()
    }

}

object ReflexConsumer extends App {

  val bootstrapServers = "localhost:9092"
  val consumerGroup = "Reflex_SCG"
  val kafkaTopics = "topic1"

  implicit val system = ActorSystem("ReflexConsumer")
  implicit val materializer = ActorMaterializer.create(system)
  implicit val ec = system.dispatcher
  
  // Sample filter function, skip messages longer than 10 chars
  def filterFunction(r: ReflexMessage) : Boolean = {
    val res = (r.event.text.size <= 10)
    system.log.info("Filtering: " + r + " => " + res)
    if (res == false) system.log.debug(r.event + " was filtered as text holds more than 10 chars")
    res
  }
  
  val rejectActor = system.actorOf(RejectActor.props, "RejectActor")
  val displayActor = system.actorOf(DisplayActor.props, "DisplayActor")
  val filterActor = system.actorOf(FilterActor.props(filterFunction, displayActor, rejectActor), "FilterActor")
  val controller = new BusinessController(filterActor, rejectActor)

  val settings = new KafkaSettings(bootstrapServers, consumerGroup, kafkaTopics)
  val reflexSource = new ReflexConsumer(settings, controller)

  // explicit commit
  reflexSource.kafkaSource.runWith(Sink.ignore)

}




/*
trait KafkaConsumer {

  val bootstrapServers = "localhost:9092"

  val system = ActorSystem("ReflexConsumer")
  implicit val ec = system.dispatcher
  implicit val m = ActorMaterializer.create(system)

  val maxPartitions = 100

  val config = system.settings.config.getConfig("akka.kafka.consumer")
  val consumerSettings =
    ConsumerSettings(config, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId("group1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val consumerSettingsWithAutoCommit =
    consumerSettings
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")

  val producerSettings = ProducerSettings(system, new StringSerializer, new ByteArraySerializer)
    .withBootstrapServers(bootstrapServers)

  def business[T] = Flow[T]
  def businessLogic(record: ConsumerRecord[String, Array[Byte]]): Future[Done] = ???

  def terminateWhenDone(result: Future[Done]): Unit =
    result.onComplete {
      case Failure(e) =>
        system.log.error(e, e.getMessage)
        system.terminate()
      case Success(_) => system.terminate()
    }

}

class AtMostOnceConsumer extends KafkaConsumer {

  def business(key: String, value: Array[Byte]): Future[Done] = ???

  def consumerControl(topic: String) =
    Consumer
      .atMostOnceSource(consumerSettings, Subscriptions.topics(topic))
      .mapAsync(1)(record => business(record.key, record.value()))
      .to(Sink.foreach(it => println(s"Done with $it")))
      .run()
}

// Consume messages at-most-once
object AtMostOnceExample extends AtMostOnceConsumer {

  def main(args: Array[String]): Unit = {
    val control = consumerControl("users")
    terminateWhenDone(control.shutdown())
  }

  override def business(key: String, value: Array[Byte]): Future[Done] = Future {
    println(key + " => " + value.toString())
    Done
  }
}

class AtLeastOnceConsumer extends KafkaConsumer {

  def business(key: String, value: Array[Byte]): Future[Done] = ???

  def consumerControl(topic: String) =
    Consumer
      .committableSource(consumerSettings, Subscriptions.topics(topic))
      .mapAsync(10) { msg =>
        business(msg.record.key, msg.record.value).map(_ => msg.committableOffset)
      }
      .mapAsync(5)(offset => offset.commitScaladsl())
      .toMat(Sink.ignore)(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()

}

// Consume messages at-least-once
object AtLeastOnceExample extends AtLeastOnceConsumer {

  def main(args: Array[String]): Unit = {
    val control = consumerControl("users")
    terminateWhenDone(control.drainAndShutdown())
  }

  override def business(key: String, value: Array[Byte]): Future[Done] = Future {
    println(key + " => " + value.toString())
    Done
  }
}

// Backpressure per partition with batch commit
object ConsumerWithPerPartitionBackpressure extends KafkaConsumer {
  def main(args: Array[String]): Unit = {
    // #committablePartitionedSource
    val control = Consumer
      .committablePartitionedSource(consumerSettings, Subscriptions.topics("topic1"))
      .flatMapMerge(maxPartitions, _._2)
      .via(business)
      .map(_.committableOffset)
      .batch(max = 100, CommittableOffsetBatch.apply)(_.updated(_))
      .mapAsync(3)(_.commitScaladsl())
      .to(Sink.ignore)
      .run()
    // #committablePartitionedSource

    terminateWhenDone(control.shutdown())
  }
}

//externally controlled kafka consumer
object ExternallyControlledKafkaConsumer extends KafkaConsumer {
  def main(args: Array[String]): Unit = {
    // #consumerActor

    //Consumer is represented by actor
    val consumer: ActorRef = system.actorOf(KafkaConsumerActor.props(consumerSettings))

    //Manually assign topic partition to it
    val controlPartition1 = Consumer
      .plainExternalSource[String, Array[Byte]](
        consumer,
        Subscriptions.assignment(new TopicPartition("topic1", 1)))
      .via(business)
      .to(Sink.ignore)
      .run()

    //Manually assign another topic partition
    val controlPartition2 = Consumer
      .plainExternalSource[String, Array[Byte]](
        consumer,
        Subscriptions.assignment(new TopicPartition("topic1", 2)))
      .via(business)
      .to(Sink.ignore)
      .run()

    consumer ! KafkaConsumerActor.Stop
    // #consumerActor
    terminateWhenDone(controlPartition1.shutdown().flatMap(_ => controlPartition2.shutdown()))
  }
}
*/
