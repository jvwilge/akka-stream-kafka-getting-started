package net.jvw

import java.time.Instant

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.Await
import scala.concurrent.duration._

object Main {

  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load()
    implicit val system = ActorSystem.create("akka-stream-kafka-getting-started", config)
    implicit val mat = ActorMaterializer()

    val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId("jeroen-akka-stream-kafka-test")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    Consumer.committableSource(consumerSettings, Subscriptions.topics("YadaYadaTopic"))
      .map(msg => {
        println(msg)
      })
      .runWith(Sink.ignore)

    // prevent WakeupException on quick restarts of vm
    scala.sys.addShutdownHook {
      println("Terminating... - " + Instant.now)
      system.terminate()
      Await.result(system.whenTerminated, 30 seconds)
      println("Terminated... Bye - " + Instant.now)
    }
  }

}