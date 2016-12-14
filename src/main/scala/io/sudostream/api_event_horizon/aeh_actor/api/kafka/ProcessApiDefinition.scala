package io.sudostream.api_event_horizon.aeh_actor.api.kafka

import akka.Done
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerMessage, ProducerSettings, Subscriptions}
import akka.stream.Materializer
import io.sudostream.api_event_horizon.messages.GeneratedTestsEvent
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.{ExecutionContextExecutor, Future}

trait ProcessApiDefinition {

  implicit def executor: ExecutionContextExecutor

  implicit val system: ActorSystem
  implicit val materializer: Materializer

  def kafkaConsumerBootServers: String

  def kafkaProducerBootServers: String

  def consumerSettings: ConsumerSettings[Array[Byte], GeneratedTestsEvent]

  def producerSettings: ProducerSettings[Array[Byte], String]

  def logger: LoggingAdapter

  def publishStuffToKafka() : Future[Done] = {
    val done = Consumer.committableSource(consumerSettings, Subscriptions.topics("generated-test-script"))
      .map {
        msg =>
          println("Test Script = " + msg.record.value())

          ProducerMessage.Message(new ProducerRecord[Array[Byte], String](
            "test-results",
            msg.record.value + "::: Has been tested"
          ), msg.committableOffset)
      }.runWith(Producer.commitableSink(producerSettings))

    done
  }

}
