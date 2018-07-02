package fr.edf.dco.ma.reflex

import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import play.api.libs.json.{Format, Json, Reads, Writes}

object ReflexProtocol {
  
  // Actor messages
  case class ReflexMessage(topic: String, partition: Int, offset: Long, createTime: Long, key: String, event: ReflexEvent)
  case class SpuriousMessage(topic: String, partition: Int, offset: Long, createTime: Long, key: String, value: String)
  
  // Core message format  
  case class ReflexEvent(text: String)
  implicit val ReflexMsgFrmt: Format[ReflexEvent] = Json.format[ReflexEvent]
}

class JsonDeserializer[A: Reads] extends Deserializer[A] {

  private val stringDeserializer = new StringDeserializer

  override def configure(configs: util.Map[String, _], isKey: Boolean) =
    stringDeserializer.configure(configs, isKey)

  override def deserialize(topic: String, data: Array[Byte]): A = {
    val ds = stringDeserializer.deserialize(topic, data)
    Json.parse(ds).as[A]
  }
  
  override def close() =
    stringDeserializer.close()
}

class JsonSerializer[A: Writes] extends Serializer[A] {

  private val stringSerializer = new StringSerializer

  override def configure(configs: util.Map[String, _], isKey: Boolean) =
    stringSerializer.configure(configs, isKey)

  override def serialize(topic: String, data: A) =
    stringSerializer.serialize(topic, Json.stringify(Json.toJson(data)))

  override def close() =
    stringSerializer.close()

}
