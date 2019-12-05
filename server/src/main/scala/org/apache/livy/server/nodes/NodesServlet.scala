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

package org.apache.livy.server.nodes

import org.apache.livy.server.JsonServlet
import org.apache.livy.sessions.{Session, SessionManager}
import org.apache.livy.sessions.Session.RecoveryMetadata

/**
 * Base servlet for getting all nodes info of livy server
 */
class NodesServlet[S <: Session, R <: RecoveryMetadata](
    private[livy] val sessionManager: SessionManager[S, R]) extends JsonServlet {

  override def shutdown(): Unit = {
    sessionManager.shutdown()
  }

  before() {
    contentType = "application/json"
  }

  get("/nodes") {
    if (sessionManager.serviceWatch.isDefined) {
      Map("nodes" -> sessionManager.serviceWatch.get.getNodes)
    } else {
      Map("nodes" -> Set(s"${request.getServerName}:${request.getServerPort}"))
    }
  }

}
