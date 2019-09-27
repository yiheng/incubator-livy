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

package org.apache.livy.thriftserver

import java.util
import java.util.UUID

import org.apache.hive.service.cli.SessionHandle
import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.livy.LivyConf
import org.apache.livy.server.interactive.InteractiveSession
import org.apache.livy.sessions.InteractiveSessionManager
import org.apache.livy.thriftserver.recovery.ThriftSessionStore
import org.scalatest.{FunSpec, Matchers}
import org.mockito.Mockito.{verify, when}
import org.scalatest.mock.MockitoSugar.mock

import scala.util.{Failure, Try}

class LivyThriftSessionManagerSpec extends FunSpec with Matchers {
  private val conf = new LivyConf()
  conf.set(LivyConf.LIVY_SPARK_VERSION, "2.0.0")


  describe("Recovery") {
    it("should not fail if state store is empty") {
      val sessionStore = mock[ThriftSessionStore]
      when(sessionStore.getAllSessions()).thenReturn(Seq.empty)

      val server = mock[LivyThriftServer]
      when(server.livyConf).thenReturn(conf)

      val thriftSessionManager = new LivyThriftSessionManager(server, conf, sessionStore)
      thriftSessionManager.getSessions.isEmpty should be(true)
    }

    it("should recover thrift sessions from state store") {
      val validMetadata = List(makeSessionMetadata(0), makeSessionMetadata(77)).map(Try(_))
      val mockLivySessions = List(Some(mock[InteractiveSession]), None)

      val invalidMetadata = List(Failure(new Exception("Fake invalid metadata")))
      val sessionStore = mock[ThriftSessionStore]
      when(sessionStore.getAllSessions()).thenReturn(validMetadata ++ invalidMetadata)

      val server = mock[LivyThriftServer]
      when(server.livyConf).thenReturn(conf)
      val sessionManager = mock[InteractiveSessionManager]
      when(server.livySessionManager).thenReturn(sessionManager)

      validMetadata.zip(mockLivySessions).foreach { case (m, s) =>
        when(sessionManager.get(m.get.id)).thenReturn(s)
        when(sessionStore.getStatements(m.get.id)).thenReturn(Seq.empty)
      }

      val thriftSessionManager = new LivyThriftSessionManager(server, conf, sessionStore)
      thriftSessionManager.getSessions.size shouldBe (validMetadata.size - 1)
    }

    it("should delete thrift session from state store") {
      val sessionHandle = makeSessionHandle()
      val sessionId = 0
      val server = mock[LivyThriftServer]
      when(server.livyConf).thenReturn(conf)

      val sessionStore = mock[ThriftSessionStore]
      when(sessionStore.getAllSessions()).thenReturn(
        List(makeSessionMetadata(sessionId, sessionHandle)).map(Try(_)))

      val mockSession = mock[InteractiveSession]

      val sessionManager = mock[InteractiveSessionManager]
      when(sessionManager.get(sessionId)).thenReturn(Some(mockSession))
      when(sessionStore.getStatements(sessionId)).thenReturn(Seq.empty)
      when(server.livySessionManager).thenReturn(sessionManager)

      val thriftSessionManager = new LivyThriftSessionManager(server, conf, sessionStore)
      thriftSessionManager.closeSession(sessionHandle)
      thriftSessionManager.getSessionInfo(sessionHandle) should be(null)
      verify(sessionStore).removeSession(sessionId)
    }
  }

  private def makeSessionMetadata(sessionId: Int): ThriftSessionRecoveryMetadata = {
    makeSessionMetadata(
      sessionId,
      UUID.randomUUID(),
      UUID.randomUUID())
  }

  private def makeSessionMetadata(sessionId: Int, handle: SessionHandle): ThriftSessionRecoveryMetadata = {
    makeSessionMetadata(
      sessionId,
      handle.getHandleIdentifier.getPublicId,
      handle.getHandleIdentifier.getSecretId)
  }

  private def makeSessionMetadata(
      sessionId: Int,
      publicId: UUID,
      secretId: UUID): ThriftSessionRecoveryMetadata = {
    new ThriftSessionRecoveryMetadata(
      sessionId,
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1.getValue,
      publicId,
      secretId,
      "user",
      "127.0.0.1",
      new util.ArrayList[String](),
      System.nanoTime()
    )
  }

  private def makeSessionHandle(): SessionHandle = {
    new SessionHandle(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1
    )
  }
}
