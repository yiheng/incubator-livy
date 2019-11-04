/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.livy.server

import javax.servlet._
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.livy.sessions.InteractiveSessionManager

private[livy] class RouteFilter(sessionManager: InteractiveSessionManager)
  extends JsonServlet
  with Filter
{
  private val pattern = "[0-9]+".r
  private val SC_TEMPORARY_REDIRECT = 307

  override def init(filterConfig: FilterConfig): Unit = {}

  override def doFilter(request: ServletRequest,
                        response: ServletResponse,
                        chain: FilterChain): Unit = {
    val httpRequest = request.asInstanceOf[HttpServletRequest]
    val sessionId = pattern findFirstIn httpRequest.getRequestURI
    sessionId match {
      case None =>
        chain.doFilter(request, response)
      case _ => {
        if (sessionManager.serviceWatch.isDefined
          && !sessionManager.serviceWatch.get.contains(sessionId.get.toInt)) {
          val (host, port) = sessionManager.serviceWatch.get.search(sessionId.get.toInt)
          val finalUrl = s"http://$host:$port${httpRequest.getRequestURI}"
          val httpResponse = response.asInstanceOf[HttpServletResponse]
          httpResponse.setStatus(SC_TEMPORARY_REDIRECT);
          httpResponse.setHeader("Location", finalUrl);
        } else {
          chain.doFilter(request, response)
        }
      }
    }
  }

  override def destroy(): Unit = {}
}

