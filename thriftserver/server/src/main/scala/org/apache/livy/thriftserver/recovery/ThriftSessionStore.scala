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

import org.apache.hive.service.cli.OperationHandle
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.server.recovery.StateStore
import org.apache.livy.thriftserver.{StatementRecoveryMetadata, ThriftSessionRecoveryMetadata}

class ThriftSessionStore(
    livyConf: LivyConf,
    store: => StateStore = StateStore.get)
  extends Logging {

  private val STORE_VERSION: String = "v1"

  def saveSession(
      livySessionId: Int,
      metaData: ThriftSessionRecoveryMetadata): Unit = {
    store.set(thriftSessionMetaDataPath(livySessionId), metaData)
  }

  def removeSession(livySessionId: Int): Unit = {
    store.remove(thriftSessionPath(livySessionId))
  }

  def getAllSessions(): Seq[Try[ThriftSessionRecoveryMetadata]] = {
    store.getChildren(thriftSessionPath)
      .flatMap { c => Try(c.toInt).toOption }
      .flatMap { id =>
        val path = thriftSessionMetaDataPath(id)
        try {
          store.get[ThriftSessionRecoveryMetadata](path).map(Success(_))
        } catch {
          case NonFatal(e) => Some(Failure(new IOException(
            s"Error getting thrift session $path", e)))
        }
      }
  }

  def saveStatement(livySessionId: Int, metadata: StatementRecoveryMetadata): Unit = {
    store.set(statementPath(livySessionId, metadata.publicId.toString), metadata)
  }

  def removeStatement(livySessionId: Int, statementId: String): Unit = {
    store.remove(statementPath(livySessionId, statementId))
  }

  def getStatements(livySessionId: Int): Seq[Try[StatementRecoveryMetadata]] = {
    store.getChildren(statementPath(livySessionId))
      .filter(s => s != null && !s.isEmpty)
      .flatMap { statementId =>
        try {
          store.get[StatementRecoveryMetadata](
            statementPath(livySessionId, statementId)).map(Success(_))
        } catch {
          case NonFatal(e) => Some(Failure(
            new IOException(
              s"Error getting statement ${statementPath(livySessionId, statementId)}",
              e)))
        }
      }
  }

  private def thriftSessionPath: String =
    s"$STORE_VERSION/thrift"

  private def thriftSessionPath(livySessionId: Int): String =
    s"$STORE_VERSION/thrift/$livySessionId"

  private def thriftSessionMetaDataPath(livySessionId: Int): String = {
    s"$STORE_VERSION/thrift/$livySessionId/metadata"
  }

  private def statementPath(livySessionId: Int): String = {
    s"$STORE_VERSION/thrift/$livySessionId/statements"
  }

  private def statementPath(livySessionId: Int, statementId: String): String = {
    s"$STORE_VERSION/thrift/$livySessionId/statements/$statementId"
  }
}
