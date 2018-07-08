package fr.edf.dco.ma.reflex

import java.util.{ UUID, Calendar }
import scala.io.Source

/*
{
  "event_id": "event_id1",
  "event_type": "event_type1",
  "event_source": "event_source_large_connector",
  "event_time": 1513766240000,
  "event_parents": ["event_idX", "event_idY"],
  "event_technical": {
    "app1": {
      "meta_tech1": "value1",
      "meta_tech2": "value2"
    },
    "app2": {
      "meta_tech3": "value3 with spaces",
      "meta_tech4": "value4"
    }
  },
  "event_data": {
    "data_label0": null,
    "data_label1": "Fill here with sufficiently large text data to simulate large JSON file (not done presently to keep jar size in check)",
    "nested_data": {
      "nested_data_label2": "value2",
      "nested_data_label3": "value3",
      "nested_data_label4": null
    }
  }
}
*/


case class EventData(data: Any)
case class EventId(uuid: UUID = UUID.randomUUID()) {
  def toJString: String = s"""event_id"": ""${uuid}"""
}
case class EventTS(now: Calendar = Calendar.getInstance) {
  def toJString: String = s"""event_time"": ""${now}"""
}
case class EventTech(appName: String, props: List[(String, String)])
case class Envelope(id: EventId, typ: String, src: String, timestamp: EventTS, parents: List[String], technical: Option[EventTech])

class Event(env: Envelope, data: Option[EventData]) {

}

object UUIDGenerator {
  val currentBase: UUID = UUID.randomUUID
  var currentIncr: Long = 0

  /*
  The most significant long consists of the following unsigned fields:

  0xFFFFFFFF00000000 time_low
  0x00000000FFFF0000 time_mid
  0x000000000000F000 version
  0x0000000000000FFF time_hi

  The least significant long consists of the following unsigned fields:
  0xC000000000000000 variant
  0x3FFF000000000000 clock_seq
  0x0000FFFFFFFFFFFF node
  */

  def incr(base: Option[UUID], seed: Option[Long]): UUID = {

    val olong = seed.getOrElse(base.getOrElse(currentBase).getMostSignificantBits)
    val mlong = (olong & 0x00000000FFFFFFFF) + (currentIncr << 32)
    val llong = base.getOrElse(currentBase).getLeastSignificantBits

    currentIncr += 1

    new UUID(mlong, llong)
  }

  def incr(base: UUID): UUID =
    new UUID(base.getMostSignificantBits + 0x0000000100000000L, base.getLeastSignificantBits)
}

class EventGenerator {

  // UUID (ex. 048098ef-4e85-4d39-b776-c6399acbdfa4) generators
  def genRandomUUID: UUID = UUID.randomUUID()
  def genIncrementalUUID(base: Option[UUID] = None, seed: Option[Long] = None): UUID = UUIDGenerator.incr(base, seed)
  def genControledUUID(mlong: Long, llong: Long): UUID = new UUID(mlong, llong)
  def genIncrementedUUID(base: UUID): UUID = UUIDGenerator.incr(base)
}

object EventGenerator extends EventGenerator with App {

  val uuid = UUID.randomUUID()
  println(uuid)

  for (i <- 1 to 8) {
    val uuid = genIncrementalUUID(None, None)
    println(uuid)
  }

  for (i <- 1 to 8) {
    val uuid = genIncrementalUUID(None, Some(0xfeed0000deadbeefL))
    println(uuid)
  }

  var base = genControledUUID(0xfeed0000deadbeefL, 0xcafebabe00facadeL)
  println(base)
  for (i <- 1 to 8) {
    base = genIncrementedUUID(base)
    println(base)
  }

  def buildEvent: Event = {
    val id = new EventId
    val ts = new EventTS
    val en = Envelope(id, "", "", ts, Nil, None)
    new Event(en, None)
  }

  def buildLinkedEvents: List[Event] = {
    Nil
  }

  val eid = new EventId()
  val ets = new EventTS()
  val env = new Envelope(eid, "TestEvent", "EventGenerator", ets, Nil, None)
  val data = new EventData("""id"":1234, ""value"":false""")
  val evt = new Event(env, Some(data))
}