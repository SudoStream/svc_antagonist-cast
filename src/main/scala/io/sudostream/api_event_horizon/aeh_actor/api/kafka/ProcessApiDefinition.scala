package io.sudostream.api_event_horizon.aeh_actor.api.kafka

import akka.Done
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.kafka.ConsumerMessage.{Committable, CommittableMessage}
import akka.kafka.ProducerMessage.Message
import akka.kafka._
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import io.sudostream.api_event_horizon.aeh_actor.business.HttpRunner
import io.sudostream.api_event_horizon.messages.GeneratedTestsEvent
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

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

    val flow =
      Flow[ConsumerMessage.CommittableMessage[Array[Byte], GeneratedTestsEvent]]
        .map {
          msg =>
            val testScript = msg.record.value()
            println("Test Script = " + testScript)

            val testResults = runTestScript(testScript)

            ProducerMessage.Message(
              new ProducerRecord[Array[Byte], String]("test-results", testResults),
              msg.committableOffset)
        }

    source.via(flow).runWith(sink)
  }

  def runTestScript(testScript: GeneratedTestsEvent): String = {
    val httpRunner = new HttpRunner

    runTest(TestToRun("helloThere"))
  }

  def runTest(testToRun: TestToRun): String = {
    val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = "http://akka.io"))
    val res = Await.result(responseFuture, 5 seconds)
    res.toString()
  }

  case class TestToRun(testName: String)

}
