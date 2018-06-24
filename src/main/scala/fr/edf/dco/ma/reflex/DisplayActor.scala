package fr.edf.dco.ma.reflex

import akka.actor.{Actor, ActorLogging, Props}
import fr.edf.dco.ma.reflex.ReflexProtocol.ReflexMessage

object DisplayActor {
  def props: Props = Props[DisplayActor]
}

class DisplayActor extends Actor with ActorLogging {

  override def receive: Receive = {
    case r: ReflexMessage => log.info(s"Message lu : ${r.text}")
  }
}
