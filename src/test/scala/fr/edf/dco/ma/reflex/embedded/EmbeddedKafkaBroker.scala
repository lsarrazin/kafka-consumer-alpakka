package fr.edf.dco.ma.reflex.embedded

import java.io.{File, IOException}
import java.nio.file.Files
import java.util.Properties

import akka.actor.ActorSystem
import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.commons.io.FileUtils

import scala.concurrent.ExecutionContextExecutor

import kafka.admin.TopicCommand
import kafka.utils.ZkUtils
import org.apache.kafka.common.security.JaasUtils

/**
  *
  * Small Kafka standaolone broker mainly to perform unit testing without external server running.
  * Thanks to https://github.com/chbatey for most of the code !
  *
  * @param kafkaPort The port the Kafka broker will listen to.
  * @param zkPort The port the Zookeeper instance will listen to.
  */
class EmbeddedKafkaBroker(kafkaPort: Int, zkPort: Int)(implicit system: ActorSystem) {

  var zookeeper: EmbeddedZookeeper = _
  var logDir: File = _
  var kafkaBrokerConfig: Properties = new Properties()
  var broker: KafkaServerStartable = _

  def startup(): Unit = {
    zookeeper = new EmbeddedZookeeper(zkPort)
    zookeeper.startup()
    try
      logDir = Files.createTempDirectory("kafka").toFile
    catch {
      case e: IOException =>
        throw new RuntimeException("Unable to start Kafka", e)
    }
    logDir.deleteOnExit()
    Runtime.getRuntime.addShutdownHook(new Thread(getDeleteLogDirectoryAction))
    kafkaBrokerConfig.setProperty("zookeeper.connect", s"localhost:$zkPort")
    kafkaBrokerConfig.setProperty("broker.id", "1")
    kafkaBrokerConfig.setProperty("host.name", "localhost")
    kafkaBrokerConfig.setProperty("port", Integer.toString(kafkaPort))
    kafkaBrokerConfig.setProperty("log.dir", logDir.getAbsolutePath)
    kafkaBrokerConfig.setProperty("log.flush.interval.messages", String.valueOf(1))
    kafkaBrokerConfig.setProperty("delete.topic.enable", String.valueOf(true))
    kafkaBrokerConfig.setProperty("offsets.topic.replication.factor", String.valueOf(1))
    kafkaBrokerConfig.setProperty("auto.create.topics.enable", String.valueOf(false))
    broker = new KafkaServerStartable(new KafkaConfig(kafkaBrokerConfig))
    broker.startup
  }

  private def getDeleteLogDirectoryAction = new Runnable() {
    override def run(): Unit = {
      if (logDir != null) try
        FileUtils.deleteDirectory(logDir)
      catch {
        case e: IOException =>
          system.log.warning("Problems deleting temporary directory " + logDir.getAbsolutePath, e)
      }
    }
  }

  def createTopic(topicName: String): Unit = {
    createTopic(topicName, 1)
  }

  def createTopic(topicName: String, numPartitions: Int): Unit = { // setup
    val arguments = new Array[String](9)
    arguments(0) = "--create"
    arguments(1) = "--zookeeper"
    arguments(2) = s"localhost:$zkPort"
    arguments(3) = "--replication-factor"
    arguments(4) = "1"
    arguments(5) = "--partitions"
    arguments(6) = String.valueOf(numPartitions)
    arguments(7) = "--topic"
    arguments(8) = topicName
    val opts = new TopicCommand.TopicCommandOptions(arguments)
    val zkUtils = ZkUtils.apply(opts.options.valueOf(opts.zkConnectOpt), 30000, 30000, JaasUtils.isZkSecurityEnabled)
    try { // run
      system.log.info(s"Executing: CreateTopic $topicName")
      TopicCommand.createTopic(zkUtils, opts)
    } finally zkUtils.close()
  }

  def shutdown(): Unit = {
    if (broker != null) {
      broker.shutdown
      broker.awaitShutdown
    }
    if (zookeeper != null) zookeeper.shutdown()
  }
}
