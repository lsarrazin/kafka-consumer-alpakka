package fr.edf.dco.ma.reflex.embedded

import java.io.{File, IOException}
import java.net.InetSocketAddress
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import org.slf4j.LoggerFactory

/**
  *
  * Small Zookeeper standaolone server mainly to perform unit testing without external server running.
  * Thanks to https://github.com/chbatey for most of the code !
  *
  * @param port The port the Zookeeper server will listen to.
  */
class EmbeddedZookeeper(port: Int) {

  private val maxConnections: Int = 16
  private var factory: NIOServerCnxnFactory = null

  val logger = LoggerFactory.getLogger("EmbeddedZookeeper")

  def startup(): Unit = {

    var snapshotDir: File = null
    var logDir: File = null
    try {
      snapshotDir = Files.createTempDirectory("zookeeper-snapshot").toFile
      logDir = java.nio.file.Files.createTempDirectory("zookeeper-logs").toFile
    } catch {
      case e: IOException =>
        throw new RuntimeException("Unable to start Kafka", e)
    }


    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        try {
          FileUtils.deleteDirectory(snapshotDir)
          FileUtils.deleteDirectory(logDir)
        } catch {
          case e: IOException =>
            logger.warn("Problems deleting temporary directory " + logDir.getAbsolutePath, e)
        }
      }
    })

    try {
      val tickTime = 500
      val zkServer = new ZooKeeperServer(snapshotDir, logDir, tickTime)
      factory = new NIOServerCnxnFactory
      factory.configure(new InetSocketAddress("localhost", port), maxConnections)
      factory.startup(zkServer)
    } catch {
      case e: InterruptedException =>
        Thread.currentThread.interrupt()
      case e: IOException =>
        throw new RuntimeException("Unable to start ZooKeeper", e)
    }
  }

  def shutdown(): Unit = {
    factory.shutdown()
  }

}



