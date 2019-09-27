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
import org.apache.livy.thriftserver.ThriftSessionRecoveryMetadata

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

class LivyThriftSessionStore(
    livyConf: LivyConf,
    store: => StateStore = StateStore.get)
  extends Logging {

  private val STORE_VERSION: String = "v1"

  def save(
      livySessionId: Int,
      thriftSession: SessionHandle,
      metaData: ThriftSessionRecoveryMetadata): Unit = {
    store.set(sessionPath(livySessionId, thriftSession), metaData)
  }

  def remove(livySessionId: Int, thriftSession: SessionHandle): Unit = {
    store.remove(sessionPath(livySessionId, thriftSession))
  }

  def getAllSessions(): Seq[Try[ThriftSessionRecoveryMetadata]] = {
    store.getChildren(sessionPath)
      .flatMap { c => Try(c.toInt).toOption }
      .flatMap { id =>
        store.getChildren(sessionPath(id)).map { d => s"${sessionPath(id)}/$d" }
      }.flatMap { path =>
        try {
          store.get[ThriftSessionRecoveryMetadata](path).map(Success(_))
        } catch {
          case NonFatal(e) => Some(Failure(new IOException(s"Error getting session $path", e)))
        }
      }
  }

  def saveOperation(thriftSession: SessionHandle, operation: OperationHandle): Unit = {

  }

  private def sessionPath: String =
    s"$STORE_VERSION/thrift"

  private def sessionPath(id: Int): String =
    s"$STORE_VERSION/thrift/$id"

  private def sessionPath(id: Int, thriftSession: SessionHandle): String = {
    val publicId = thriftSession.getHandleIdentifier.getPublicId
    val secretId = thriftSession.getHandleIdentifier.getSecretId
    s"$STORE_VERSION/thrift/$id/$publicId-$secretId"
  }
}
