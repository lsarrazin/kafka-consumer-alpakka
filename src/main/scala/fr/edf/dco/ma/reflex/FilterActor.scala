package fr.edf.dco.ma.reflex

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import com.typesafe.config.Config

import FilterActor.StopWorking
import ReflexProtocol.ReflexMessage
import org.apache.kafka.clients.consumer.ConsumerRecords


object FilterActor {
  def props(filterFunction: ReflexMessage => Boolean, processorActor: ActorRef, rejectActor: ActorRef): Props = Props(new FilterActor(filterFunction, processorActor, rejectActor))

  case object StopWorking
}

class FilterActor(filterFunction: ReflexMessage => Boolean, processorActor: ActorRef, rejectActor: ActorRef) 
  extends Actor with ActorLogging {

  override def receive: Receive = {

    case StopWorking =>
      self ! PoisonPill
    
    case m: ReflexMessage =>
      log.info("Found something to filter !")
      if (filterFunction(m)) {
        processorActor ! m
      } else {
        rejectActor ! m
      }

    case unknown =>
      log.error(s"Got unknown message: $unknown")

  }
}
