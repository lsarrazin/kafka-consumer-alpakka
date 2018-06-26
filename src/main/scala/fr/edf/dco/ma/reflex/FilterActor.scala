package fr.edf.dco.ma.reflex

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import com.typesafe.config.Config

import FilterActor.StopWorking
import ReflexProtocol.ReflexMessage


object FilterActor {
  def props(filterFunction: ReflexMessage => Boolean, processorActor: ActorRef): Props = Props(new FilterActor(filterFunction, processorActor))

  case object StopWorking
}

class FilterActor(val filterFunction: ReflexMessage => Boolean, val processorActor: ActorRef) 
  extends Actor with ActorLogging {

  //for pattern matching in our receive method
  val msgExtractor = ConsumerRecords.extractor[java.lang.String, ReflexMessage]
  
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
