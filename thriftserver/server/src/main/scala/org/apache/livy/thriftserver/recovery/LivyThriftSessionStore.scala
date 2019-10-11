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

import java.io.IOException

import org.apache.hive.service.cli.{OperationHandle, SessionHandle}
import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.server.recovery.StateStore
import org.apache.livy.thriftserver.{StatementOperationRecoveryMetadata, ThriftSessionRecoveryMetadata}

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

class LivyThriftSessionStore(
    livyConf: LivyConf,
    store: => StateStore = StateStore.get)
  extends Logging {

  private val STORE_VERSION: String = "v1"

  def save(
      livySessionId: Int,
      metaData: ThriftSessionRecoveryMetadata): Unit = {
    store.set(thriftSessionMetaDataPath(livySessionId), metaData)
  }

  def remove(livySessionId: Int): Unit = {
    store.remove(thriftSessionPath(livySessionId))
  }

  def removeStatement(livySessionId: Int, operationHandle: OperationHandle): Unit = {
    store.remove(statementPath(livySessionId,
      operationHandle.getHandleIdentifier.getPublicId.toString))
  }

  def getAllSessions(): Seq[Try[ThriftSessionRecoveryMetadata]] = {
    store.getChildren(thriftSessionPath)
      .flatMap { c => Try(c.toInt).toOption }
      .flatMap { id =>
        store.getChildren(thriftSessionPath(id)).map { d => s"${thriftSessionPath(id)}/$d" }
      }.flatMap { path =>
        try {
          store.get[ThriftSessionRecoveryMetadata](path).map(Success(_))
        } catch {
          case NonFatal(e) => Some(Failure(new IOException(s"Error getting session $path", e)))
        }
      }
  }

  def getStatements(livySessionId: Int): Seq[Try[StatementOperationRecoveryMetadata]] = {
    store.getChildren(statementPath(livySessionId))
      .flatMap { statementId =>
        try {
          store.get[StatementOperationRecoveryMetadata](
            statementPath(livySessionId, statementId)).map(Success(_))
        } catch {
          case NonFatal(e) => Some(Failure(
            new IOException(
              s"Error getting session ${statementPath(livySessionId, statementId)}",
              e)))
        }
      }
  }

  def saveStatement(
      livySessionId: Int,
      metadata: StatementOperationRecoveryMetadata): Unit = {
    store.set(statementPath(livySessionId, metadata.publicId.toString), metadata)
  }

  private def thriftSessionPath: String =
    s"$STORE_VERSION/thrift"

  private def thriftSessionPath(id: Int): String =
    s"$STORE_VERSION/thrift/$id"

  private def thriftSessionMetaDataPath(id: Int): String = {
    s"$STORE_VERSION/thrift/$id/metadata"
  }

  private def statementPath(id: Int): String = {
    s"$STORE_VERSION/thrift/$id/statements"
  }

  private def statementPath(id: Int, statementId: String): String = {
    s"$STORE_VERSION/thrift/$id/statements/$statementId"
  }
}
