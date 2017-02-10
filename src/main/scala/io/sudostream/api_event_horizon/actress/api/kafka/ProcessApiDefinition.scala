package io.sudostream.api_event_horizon.actress.api.kafka

import akka.Done
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import akka.kafka.ConsumerMessage.{Committable, CommittableMessage}
import akka.kafka.ProducerMessage.Message
import akka.kafka._
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import io.sudostream.api_event_horizon.messages.{SpeculativeScreenplay, HttpMethod}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

trait ProcessApiDefinition {

  implicit def executor: ExecutionContextExecutor

  implicit val system: ActorSystem
  implicit val materializer: Materializer

  def kafkaConsumerBootServers: String
  def kafkaProducerBootServers: String
  def consumerSettings: ConsumerSettings[Array[Byte], SpeculativeScreenplay]
  def producerSettings: ProducerSettings[Array[Byte], String]
  def logger: LoggingAdapter

  def publishStuffToKafka(): Future[Done] = {
    val source: Source[CommittableMessage[Array[Byte], SpeculativeScreenplay], Control] =
      Consumer.committableSource(consumerSettings, Subscriptions.topics("speculative-screenplays"))

    val sink: Sink[Message[Array[Byte], String, Committable], Future[Done]] =
      Producer.commitableSink(producerSettings)

    val flow =
      Flow[ConsumerMessage.CommittableMessage[Array[Byte], SpeculativeScreenplay]]
        .map {
          msg =>
            val speculativeScreenplay = msg.record.value()
            println("SpeculativeScreenplay = " + speculativeScreenplay)

            val protagonistAnswers = performTheScript(speculativeScreenplay)

            ProducerMessage.Message(
              new ProducerRecord[Array[Byte], String]("protagonist-answers", protagonistAnswers),
              msg.committableOffset)
        }

    source
      .via(flow)
      .runWith(sink)
  }

  def performTheScript(speculativeScreenplay: SpeculativeScreenplay): String = {
    val fullResults = for {httpQuestion <- speculativeScreenplay.theAntagonistInterrogation}
      yield {
        val uriUnderTest = "http://" + speculativeScreenplay.hostname + ":" + speculativeScreenplay.ports.head + "/" + httpQuestion.uriPath
        println("Testing Uri :  " + uriUnderTest)
        runTest(HttpQuestionToAsk(uriUnderTest, httpQuestion.method))
      }

    val prettyResults = fullResults mkString "\n"
    println("Tests Done:-\n" + prettyResults + "\n\n")
    prettyResults
  }

  def runTest(httpQuestion: HttpQuestionToAsk): String = {
    try {
      val responseFuture: Future[HttpResponse] = Http().singleRequest(
        HttpRequest(
          method = httpQuestion.actualMethod,
          uri = httpQuestion.uriToTest)
      )

      // NOTE: We have to block here because the entire point is to run in sequence against the target
      val res = Await.result(responseFuture, 5 seconds)

      "SUCCESS: " + httpQuestion.actualMethod + " : " + httpQuestion.uriToTest + " :: " + res.toString()
    } catch {
      case ex: Exception => "FAILURE: " + httpQuestion.actualMethod + " : " + httpQuestion.uriToTest + " :: " + ex.getMessage
    }
  }

  case class HttpQuestionToAsk(uriToTest: String, internalMethod: io.sudostream.api_event_horizon.messages.HttpMethod) {
    val actualMethod: akka.http.scaladsl.model.HttpMethod = internalMethod match {
      case HttpMethod.GET => HttpMethods.GET
      case HttpMethod.POST => HttpMethods.POST
      case HttpMethod.PUT => HttpMethods.PUT
      case HttpMethod.DELETE => HttpMethods.DELETE
      case _ => HttpMethods.GET
    }
  }

}
