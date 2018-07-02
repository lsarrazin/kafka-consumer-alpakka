package fr.edf.dco.ma.reflex

import akka.actor.{Actor, ActorLogging, Props}
import org.apache.kafka.clients.consumer.ConsumerRecord

import ReflexProtocol.ReflexMessage
import ReflexProtocol.SpuriousMessage

object RejectActor {
  def props: Props = Props[RejectActor]
}

class RejectActor extends Actor with ActorLogging {

  override def receive: Receive = {
    case m: ReflexMessage => log.warning("Filtered: " + m)
    case r: SpuriousMessage => log.error("Spurious: " + r)
    case e: Any => log.error("Weird", e)
  }
}
