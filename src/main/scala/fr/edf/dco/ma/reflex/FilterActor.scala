package fr.edf.dco.ma.reflex

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import com.typesafe.config.Config
import fr.edf.dco.ma.reflex.FilterActor.StopWorking
import fr.edf.dco.ma.reflex.ReflexProtocol.ReflexMessage


object FilterActor {
  def props(filterFunction: ReflexMessage => Boolean, topic: String, processorActor: ActorRef, config: Config): Props = Props(new FilterActor(filterFunction, topic, processorActor, config))

  case object StopWorking

}

class FilterActor(val filterFunction: ReflexMessage => Boolean, val topic: String, val processorActor: ActorRef, val config: Config) extends Actor
  with ActorLogging with ReflexConsumer {

  override def preStart() = {
    super.preStart()
    subscribe(List(topic))
  }

  override def receive: Receive = {

    case StopWorking =>
    self ! PoisonPill

    case msgExtractor(consumerRecords) =>
      consumerRecords.pairs.foreach {
        case (_, rflxMsg) =>
          log.info("Found something to filter !")
          if (filterFunction(rflxMsg)) processorActor ! rflxMsg
      }

    case unknown =>
      log.error(s"Got unknown message: $unknown")

  }
}
