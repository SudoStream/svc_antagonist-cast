package io.sudostream.api_antagonist.actress

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.Http
import akka.kafka.{ConsumerSettings, ProducerSettings}
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.{Config, ConfigFactory}
import io.sudostream.api_antagonist.actress.api.http.ProcessApiDefinition
import io.sudostream.api_antagonist.actress.api.kafka
import io.sudostream.api_antagonist.kafka.serialising.{FinalScriptDeserialiser, LiveActedLineSerializer, RollCreditsSerializer, SpeculativeScreenplayDeserialiser}
import io.sudostream.api_antagonist.messages.{FinalScript, LiveActedLine, RollCredits}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringSerializer}

import scala.concurrent.ExecutionContextExecutor

object Actress extends App with Service
  with ProcessApiDefinition
  with kafka.ProcessApiDefinition {

  override val config = ConfigFactory.load()
  override implicit val system = ActorSystem("scriptwriter-system", config)
  override implicit val executor = system.dispatcher
  override implicit val materializer = ActorMaterializer()
  override val logger = Logging(system, getClass)

  override val kafkaConsumerBootServers = config.getString("akka.kafka.consumer.bootstrapservers")
  override val kafkaProducerBootServers = config.getString("akka.kafka.producer.bootstrapservers")

  override val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new FinalScriptDeserialiser)
    .withBootstrapServers(kafkaConsumerBootServers)
    .withGroupId("akka.kafka.consumer.groupid")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

  override val producerSettingsLiveActedLine = ProducerSettings(system, new ByteArraySerializer, new LiveActedLineSerializer)
    .withBootstrapServers(kafkaProducerBootServers)

  override val producerSettingsRollCredits = ProducerSettings(system, new ByteArraySerializer, new RollCreditsSerializer)
    .withBootstrapServers(kafkaProducerBootServers)

  setUpKafkaFlow()

  Http().bindAndHandle(routes, config.getString("http.interface"), config.getInt("http.port"))

}

trait Service {

  implicit val system: ActorSystem

  implicit def executor: ExecutionContextExecutor

  implicit val materializer: Materializer
  val logger: LoggingAdapter

  def config: Config
}
