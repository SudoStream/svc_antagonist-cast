package io.sudostream.api_event_horizon.aeh_actor.api.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.util.Timeout

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

trait ProcessApiDefinition extends Health
  with io.sudostream.api_event_horizon.aeh_actor.api.kafka.ProcessApiDefinition {

  implicit def executor: ExecutionContextExecutor

  implicit val system: ActorSystem
  implicit val materializer: Materializer
  implicit val timeout = Timeout(30.seconds)

  val routes: Route = path("aeh-actor" / "testresults") {
    get {
      complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>No Results I am afraid</h1>"))
    }
  } ~ health

}
