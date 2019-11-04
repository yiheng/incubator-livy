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

package org.apache.livy.thriftserver.recovery

import java.util
import java.util.UUID

import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.livy.server.recovery.StateStore
import org.apache.livy.LivyConf
import org.apache.livy.thriftserver.{StatementRecoveryMetadata, ThriftSessionRecoveryMetadata}
import org.mockito.Mockito.{verify, when}
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.mock.MockitoSugar.mock

import scala.util.Success

class ThriftSessionStoreSpec extends FunSpec with Matchers {

  val sessionPath = "v1/thrift"
  val conf = new LivyConf()

  describe("ThriftSessionStore Thrift Sessions") {
    it("should save thrift session to state store") {
      val stateStore = mock[StateStore]
      val thriftSessionStore = new ThriftSessionStore(conf, stateStore)

      val sessionId = 1
      val metadata = testSessionMetadata(sessionId)
      thriftSessionStore.saveSession(sessionId, metadata)
      verify(stateStore).set(s"$sessionPath/$sessionId/metadata", metadata)
    }

    it("should return existing thrift sessions") {
      val stateStore = mock[StateStore]
      val thriftSessionStore = new ThriftSessionStore(conf, stateStore)

      // Mock the behavior
      val validMetadata = Map(
        "0" -> Some(testSessionMetadata(0)),
        "5" -> None,
        "77" -> Some(testSessionMetadata(77)))
      val corruptedMetadata = Map(
        "7" -> new RuntimeException("Test"),
        "11212" -> new RuntimeException("Test")
      )

      when(stateStore.getChildren(sessionPath)).thenReturn(
        (validMetadata ++ corruptedMetadata).keys.toList)

      validMetadata.foreach { case (id, m) =>
        when(stateStore.get[ThriftSessionRecoveryMetadata](s"$sessionPath/$id/metadata"))
          .thenReturn(m)
      }

      corruptedMetadata.foreach { case (id, ex) =>
        when(stateStore.get[ThriftSessionRecoveryMetadata](s"$sessionPath/$id/metadata"))
          .thenThrow(ex)
      }

      val sessions = thriftSessionStore.getAllSessions()
      // Verify normal metadata are retrieved.
      sessions.filter(_.isSuccess) should contain theSameElementsAs
        validMetadata.values.filter(_.isDefined).map(m => Success(m.get))
      // Verify exceptions are wrapped as in Try and are returned.
      sessions.filter(_.isFailure) should have size corruptedMetadata.size
    }

    it("should not throw if the state store is empty") {
      val stateStore = mock[StateStore]
      val sessionStore = new ThriftSessionStore(conf, stateStore)
      when(stateStore.getChildren(sessionPath)).thenReturn(Seq.empty)

      val s = sessionStore.getAllSessions()
      s.filter(_.isSuccess) shouldBe empty
    }

    it("should remove thrift session") {
      val stateStore = mock[StateStore]
      val sessionStore = new ThriftSessionStore(conf, stateStore)
      val id = 1

      sessionStore.removeSession(1)
      verify(stateStore).remove(s"$sessionPath/$id")
    }
  }


  describe("ThriftSessionStore Statements") {
    it("should save statement") {
      val stateStore = mock[StateStore]
      val sessionStore = new ThriftSessionStore(conf, stateStore)

      val sessionId = 1
      val metadata = testStatementMetadata(sessionId)
      sessionStore.saveStatement(sessionId, metadata)
      verify(stateStore).set(s"$sessionPath/$sessionId/statements/${metadata.publicId}", metadata)
    }

    it("should return existing thrift statements") {
      val stateStore = mock[StateStore]
      val sessionStore = new ThriftSessionStore(conf, stateStore)

      val sessionId = 1
      // Mock the behavior
      val metadata1 = testStatementMetadata(sessionId)
      val metadata2 = testStatementMetadata(sessionId)
      val validMetadata = Map(
        metadata1.publicId.toString -> Some(metadata1),
        UUID.randomUUID().toString -> None,
        metadata2.publicId.toString -> Some(metadata2))
      val corruptedMetadata = Map(
        UUID.randomUUID().toString -> new RuntimeException("Test"),
        UUID.randomUUID().toString -> new RuntimeException("Test")
      )

      when(stateStore.getChildren(s"$sessionPath/$sessionId/statements")).thenReturn(
        (validMetadata ++ corruptedMetadata).keys.toList
      )

      validMetadata.foreach { case (id, m) =>
        when(stateStore
          .get[StatementRecoveryMetadata](s"$sessionPath/$sessionId/statements/$id"))
          .thenReturn(m)
      }

      corruptedMetadata.foreach { case (id, ex) =>
        when(stateStore
          .get[StatementRecoveryMetadata](s"$sessionPath/$sessionId/statements/$id"))
          .thenThrow(ex)
      }

      val statements = sessionStore.getStatements(sessionId)
      // Verify normal metadata are retrieved.
      statements.filter(_.isSuccess) should contain theSameElementsAs
        validMetadata.values.filter(_.isDefined).map(m => Success(m.get))
      // Verify exceptions are wrapped as in Try and are returned.
      statements.filter(_.isFailure) should have size corruptedMetadata.size
    }

    it("should remove statement") {
      val stateStore = mock[StateStore]
      val sessionStore = new ThriftSessionStore(conf, stateStore)
      val sessionId = 1
      val statementId = UUID.randomUUID().toString

      sessionStore.removeStatement(1, statementId)
      verify(stateStore).remove(s"$sessionPath/$sessionId/statements/$statementId")
    }
  }

  def testSessionMetadata(sessionId: Int): ThriftSessionRecoveryMetadata = {
    new ThriftSessionRecoveryMetadata(
      sessionId,
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1.getValue,
      UUID.randomUUID(),
      UUID.randomUUID(),
      "user",
      "127.0.0.1",
      new util.ArrayList[String](),
      System.nanoTime()
    )
  }

  def testStatementMetadata(sessionId: Int): StatementRecoveryMetadata = {
    new StatementRecoveryMetadata(
      sessionId,
      "show tables",
      true,
      UUID.randomUUID(),
      UUID.randomUUID()
    )
  }
}
