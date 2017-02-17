package io.sudostream.api_antagonist.actress.api.kafka

import akka.http.scaladsl.model.HttpMethods
import io.sudostream.api_antagonist.messages.HttpMethod

case class HttpQuestionToAsk(
                              filmUuid: String,
                              finalScriptUuid: String,
                              uriToTest: String = "",
                              internalMethod: io.sudostream.api_antagonist.messages.HttpMethod = HttpMethod.GET,
                              allTestsCompleted: Boolean = false
                            ) {
  val actualMethod: akka.http.scaladsl.model.HttpMethod = internalMethod match {
    case HttpMethod.GET => HttpMethods.GET
    case HttpMethod.POST => HttpMethods.POST
    case HttpMethod.PUT => HttpMethods.PUT
    case HttpMethod.DELETE => HttpMethods.DELETE
    case _ => HttpMethods.GET
  }
}
