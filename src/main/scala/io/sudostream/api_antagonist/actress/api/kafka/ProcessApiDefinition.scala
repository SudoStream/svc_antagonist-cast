package io.sudostream.api_antagonist.actress.api.kafka

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import akka.kafka.ConsumerMessage.{Committable, CommittableMessage}
import akka.kafka.ProducerMessage.Message
import akka.kafka._
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.{ClosedShape, Materializer}
import akka.{Done, NotUsed}
import io.sudostream.api_antagonist.messages._
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

trait ProcessApiDefinition {

  implicit def executor: ExecutionContextExecutor

  implicit val system: ActorSystem
  implicit val materializer: Materializer

  def kafkaConsumerBootServers: String
  def kafkaProducerBootServers: String
  def consumerSettings: ConsumerSettings[Array[Byte], FinalScript]
  def producerSettingsLiveActedLine: ProducerSettings[Array[Byte], LiveActedLine]
  def producerSettingsRollCredits: ProducerSettings[Array[Byte], RollCredits]


  def logger: LoggingAdapter

  def setUpKafkaFlow(): NotUsed = {

    val runnableGraph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val source: Source[CommittableMessage[Array[Byte], FinalScript], Control] = definedSource

      val flowCreateRollCreditsProducer = defineCreateRollCreditsProducer
      val flowExtractFinalScript = defineExtractFinalScriptFlow
      val flowCreateHttpQuestions = defineCreatedHttpQuestionsFlow
      val flowRunHttpRequest = defineRunHttpRequestFlow

      val broadcastInitialMessage = builder.add(Broadcast[CommittableMessage[Array[Byte], FinalScript]](2))

      val sinkLiveActedLine = defineLiveActedLineSink
      val sinkRollCredits: Sink[Message[Array[Byte], RollCredits, Committable], Future[Done]] = defineRollCreditsSink


      source ~> broadcastInitialMessage ~> flowExtractFinalScript ~>
        flowCreateHttpQuestions.mapConcat(httpQuestions => httpQuestions.toList) ~>
        flowRunHttpRequest ~> sinkLiveActedLine

      broadcastInitialMessage ~> flowCreateRollCreditsProducer ~> sinkRollCredits

      ClosedShape

    })


    runnableGraph.run()

  }

  private def defineCreateRollCreditsProducer = {
    Flow[CommittableMessage[Array[Byte], FinalScript]]
      .map {
        msg =>
          val finalScript = msg.record.value()
          val rollCredits =
            RollCredits(
              filmUuid = finalScript.filmUuid,
              finalScriptUuid = finalScript.finalScriptUuid
            )

          ProducerMessage.Message(
            new ProducerRecord[Array[Byte], RollCredits]("live-acted-line", rollCredits), msg.committableOffset)
      }
  }

  private def defineRollCreditsSink = Producer.commitableSink(producerSettingsRollCredits)

  private def defineLiveActedLineSink = Producer.plainSink(producerSettingsLiveActedLine)

  private def defineRunHttpRequestFlow =
    Flow[HttpQuestionToAsk]
      .map {
        httpQuestionToAsk =>
          println("About to make http call ...")
          val liveActedLine = runTest(httpQuestionToAsk)
          println(s"Live acted line = ${liveActedLine.toString}")
          new ProducerRecord[Array[Byte], LiveActedLine]("live-acted-line", liveActedLine)
      }

  private def defineCreatedHttpQuestionsFlow = {
    Flow[FinalScript]
      .map {
        finalScript: FinalScript =>
          println("Okay so we are going to extract some questions here for http")
          extractHttpQuestions(finalScript)
      }
  }

  private def defineExtractFinalScriptFlow = {
    Flow[CommittableMessage[Array[Byte], FinalScript]]
      .map {
        msg =>
          val finalScript = msg.record.value()
          println("Ho Ho Final Script = " + finalScript)
          finalScript
      }
  }

  private def definedSource = {
    Consumer.committableSource(consumerSettings, Subscriptions.topics("final-script"))
  }

  def extractHttpQuestions(finalScript: FinalScript): Seq[HttpQuestionToAsk] =
    for {httpQuestion <- finalScript.antagonistLines}
      yield {
        val uriUnderTest = httpQuestion.uriPath
        HttpQuestionToAsk(uriUnderTest, httpQuestion.method)
      }


  def runTest(httpQuestion: HttpQuestionToAsk): LiveActedLine = {
    try {
      println(s"Http TODO ... ${httpQuestion.toString}")

      val timeStart = System.currentTimeMillis()

      val responseFuture: Future[HttpResponse] = Http().singleRequest(
        HttpRequest(
          method = httpQuestion.actualMethod,
          uri = httpQuestion.uriToTest)
      )

      // NOTE: We have to block here because the entire point is to run in sequence against the target
      val res = Await.result(responseFuture, 300 seconds)

      val timeEnd = System.currentTimeMillis()

      println("Http Done")

      // TODO: Create proper values here!
      LiveActedLine(
        filmUuid = "filmUuid",
        finalScriptUuid = "finalScriptUuid",
        uriTested = httpQuestion.uriToTest,
        responseCodeActual = res.status.intValue(),
        responseBodyActual = "",
        responseMilliseconds = timeEnd - timeStart,
        responseCodeExpected = 200,
        responseBodyExpected = "",
        responseMillisecondsSla = 10000
      )
    } catch {
      case ex: Exception => "FAILURE: " + httpQuestion.actualMethod + " : " + httpQuestion.uriToTest + " :: " + ex.getMessage
        println(s"ERROR: $ex")
        throw ex
    }
  }

  case class HttpQuestionToAsk(uriToTest: String, internalMethod: io.sudostream.api_antagonist.messages.HttpMethod) {
    val actualMethod: akka.http.scaladsl.model.HttpMethod = internalMethod match {
      case HttpMethod.GET => HttpMethods.GET
      case HttpMethod.POST => HttpMethods.POST
      case HttpMethod.PUT => HttpMethods.PUT
      case HttpMethod.DELETE => HttpMethods.DELETE
      case _ => HttpMethods.GET
    }
  }

}
