package io.sudostream.api_event_horizon.aeh_actor.api.kafka

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.kafka.ConsumerMessage.{Committable, CommittableMessage, CommittableOffset}
import akka.kafka.ProducerMessage.Message
import akka.kafka._
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}
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

  def publishStuffToKafka(): Future[Done] = {
    val source: Source[CommittableMessage[Array[Byte], GeneratedTestsEvent], Control] =
      Consumer.committableSource(consumerSettings, Subscriptions.topics("generated-test-script"))

    val sink: Sink[Message[Array[Byte], String, Committable], Future[Done]] =
      Producer.commitableSink(producerSettings)

    val flow: Flow[
      CommittableMessage[Array[Byte], GeneratedTestsEvent],
      Message[Array[Byte], String, CommittableOffset], NotUsed] =
      Flow[ConsumerMessage.CommittableMessage[Array[Byte], GeneratedTestsEvent]]
        .map {
          msg =>
            println("Test Script = " + msg.record.value())

            ProducerMessage.Message(new ProducerRecord[Array[Byte], String](
              "test-results",
              msg.record.value + "::: Has been tested"
            ), msg.committableOffset)
        }

    source.via(flow).runWith(sink)
  }

}
