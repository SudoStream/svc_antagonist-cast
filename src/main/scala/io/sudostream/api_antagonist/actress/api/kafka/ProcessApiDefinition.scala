package io.sudostream.api_antagonist.actress.api.kafka

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.kafka.ConsumerMessage.{Committable, CommittableMessage, CommittableOffset}
import akka.kafka.ProducerMessage.Message
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.scaladsl.{Broadcast, GraphDSL, RunnableGraph, Sink, Source, ZipWith}
import akka.stream.{ClosedShape, Materializer}
import akka.{Done, NotUsed}
import io.sudostream.api_antagonist.messages._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

trait ProcessApiDefinition extends ProcessApiStreamingComponents {

  implicit def executor: ExecutionContextExecutor

  implicit val system: ActorSystem
  implicit val materializer: Materializer

  def logger: LoggingAdapter

  def kafkaConsumerBootServers: String
  def kafkaProducerBootServers: String

  def setUpKafkaFlow(): NotUsed = {

    val runnableGraph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val source: Source[CommittableMessage[Array[Byte], FinalScript], Control] = definedSource

      val flowCreateRollCreditsProducer = defineCreateRollCreditsProducer
      val flowExtractFinalScript = defineExtractFinalScriptFlow
      val flowCreateHttpQuestions = defineCreatedHttpQuestionsFlow
      val flowRunHttpRequest = defineRunHttpRequestFlow

      val broadcastInitialMessage = builder.add(Broadcast[CommittableMessage[Array[Byte], FinalScript]](2))
      val broadcastLiveActedLine = builder.add(Broadcast[LiveActedLine](2))

      val sinkLiveActedLine = defineLiveActedLineSink
      val sinkRollCredits: Sink[Message[Array[Byte], RollCredits, Committable], Future[Done]] = defineRollCreditsSink

      val zip = builder.add(ZipWith((msg: Message[Array[Byte], RollCredits, CommittableOffset], trigger: LiveActedLine) => msg))

      // GRAPH
      source ~> broadcastInitialMessage ~> flowExtractFinalScript ~>
        flowCreateHttpQuestions.mapConcat(httpQuestions => httpQuestions.toList) ~>
        flowRunHttpRequest ~> broadcastLiveActedLine ~> defineCreateKafkaRecordFlow ~> echo ~> sinkLiveActedLine

      broadcastLiveActedLine
        .filter(liveActedLine => liveActedLine.allLinesCompleted) ~> zip.in1

      broadcastInitialMessage ~> flowCreateRollCreditsProducer ~> zip.in0

      zip.out ~> sinkRollCredits

      ClosedShape
    })

    runnableGraph.run()
  }


  override def extractHttpQuestions(finalScript: FinalScript): Seq[HttpQuestionToAsk] = {
    val httpQuestions = for {httpQuestion <- finalScript.antagonistLines}
      yield {
        val uriUnderTest = httpQuestion.uriPath

        HttpQuestionToAsk(
          filmUuid = finalScript.filmUuid,
          finalScriptUuid = finalScript.finalScriptUuid,
          uriToTest = uriUnderTest,
          internalMethod = httpQuestion.method)

      }

    httpQuestions ::: HttpQuestionToAsk(
      filmUuid = finalScript.filmUuid,
      finalScriptUuid = finalScript.finalScriptUuid,
      allTestsCompleted = true
    ) :: Nil

  }

  override def runTest(httpQuestion: HttpQuestionToAsk): LiveActedLine = {
    if (httpQuestion.allTestsCompleted) {
      LiveActedLine(
        filmUuid = httpQuestion.filmUuid,
        finalScriptUuid = httpQuestion.finalScriptUuid,
        allLinesCompleted = true
      )
    } else {
      val timeStart = System.currentTimeMillis()
      try {
        println(s"Http TODO ... ${httpQuestion.toString}")

        val timeStart = System.currentTimeMillis()

        val responseFuture: Future[HttpResponse] = Http().singleRequest(
          HttpRequest(
            method = httpQuestion.actualMethod,
            uri = httpQuestion.uriToTest
          )
        )

        // NOTE: We have to block here because the entire point is to run in sequence against the target
        // At least I *think* we do :-)
        val res = Await.result(responseFuture, 3 seconds)
        //
        //

        val timeEnd = System.currentTimeMillis()

        println("Http Done")

        LiveActedLine(
          filmUuid = httpQuestion.filmUuid,
          finalScriptUuid = httpQuestion.finalScriptUuid,
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

          LiveActedLine(
            filmUuid = httpQuestion.filmUuid,
            finalScriptUuid = httpQuestion.finalScriptUuid,
            uriTested = httpQuestion.uriToTest,
            responseCodeActual = -1,
            responseBodyActual = s"Runtime Exception: $ex",
            responseMilliseconds = System.currentTimeMillis() - timeStart,
            responseCodeExpected = 200,
            responseBodyExpected = "",
            responseMillisecondsSla = 3000
          )

      }
    }
  }

}
