package io.sudostream.api_antagonist.actress.api.kafka

import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerMessage, ProducerSettings, Subscriptions}
import akka.stream.scaladsl.Flow
import io.sudostream.api_antagonist.messages.{FinalScript, LiveActedLine, RollCredits}
import org.apache.kafka.clients.producer.ProducerRecord

trait ProcessApiStreamingComponents {

  def extractHttpQuestions(finalScript: FinalScript): Seq[HttpQuestionToAsk]
  def runTest(httpQuestion: HttpQuestionToAsk): LiveActedLine

  def consumerSettings: ConsumerSettings[Array[Byte], FinalScript]
  def producerSettingsLiveActedLine: ProducerSettings[Array[Byte], LiveActedLine]
  def producerSettingsRollCredits: ProducerSettings[Array[Byte], RollCredits]

  private[kafka] def defineRollCreditsSink = Producer.commitableSink(producerSettingsRollCredits)
  private[kafka] def defineLiveActedLineSink = Producer.plainSink(producerSettingsLiveActedLine)

  private[kafka] def defineCreateRollCreditsProducer = {
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

  private[kafka] def defineRunHttpRequestFlow =
    Flow[HttpQuestionToAsk]
      .map {
        httpQuestionToAsk =>
          println("About to make http call ...")
          val liveActedLine = runTest(httpQuestionToAsk)
          println(s"Live acted line = ${liveActedLine.toString}\n---\n\n")
          liveActedLine
      }

  private[kafka] def defineCreateKafkaRecordFlow =
    Flow[LiveActedLine]
      .map {
        liveActedLine =>
          new ProducerRecord[Array[Byte], LiveActedLine]("live-acted-line", liveActedLine)
      }

  private[kafka] def echo =
    Flow[ProducerRecord[Array[Byte], LiveActedLine]]
      .map {
        msgToCommit =>

          println(s"Msg: ${msgToCommit}")

          msgToCommit
      }

  private[kafka] def defineCreatedHttpQuestionsFlow = {
    Flow[FinalScript]
      .map {
        finalScript: FinalScript =>
          println("Okay so we are going to extract some questions here for http")
          extractHttpQuestions(finalScript)
      }
  }

  private[kafka] def defineExtractFinalScriptFlow = {
    Flow[CommittableMessage[Array[Byte], FinalScript]]
      .map {
        msg =>
          val finalScript = msg.record.value()
          println("Ho Ho Final Script = " + finalScript)
          finalScript
      }
  }

  private[kafka] def definedSource = {
    Consumer.committableSource(consumerSettings, Subscriptions.topics("final-script"))
  }

}
