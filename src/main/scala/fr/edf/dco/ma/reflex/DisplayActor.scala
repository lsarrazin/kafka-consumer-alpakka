package fr.edf.dco.ma.reflex

import akka.actor.{Actor, ActorLogging, Props}

import ReflexProtocol.ReflexMessage

object DisplayActor {
  def props: Props = Props[DisplayActor]
}

class DisplayActor extends Actor with ActorLogging {

  override def receive: Receive = {
    case r: ReflexMessage => log.info("Processed: " + r)
    case e: Any => log.error("Unknown", e)
  }
}
