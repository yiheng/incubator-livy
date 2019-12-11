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

object MultiActiveConstants {
  val MULTI_ACTIVE_REQ_TYPE = "multi-active.req.type"
  val MULTI_ACTIVE_REQ_TYPE_CREATE_SESSION = "multi-active.create.session"
  val MULTI_ACTIVE_REQ_TYPE_ASK_THRIFT_ADDR = "multi-active.ask.thrift.addr"

  val MULTI_ACTIVE_SESSION_ID = "multi-active.session.id"
  val MULTI_ACTIVE_THRIFT_ADDR = "multi-active.thrift.addr"
  val MULTI_ACTIVE_REDIRECT_CODE = 307

  def getRedirectMsg(sessionId: Int, thriftAddr: String): String = {
    s"$MULTI_ACTIVE_SESSION_ID=$sessionId;$MULTI_ACTIVE_THRIFT_ADDR=$thriftAddr"
  }
}

object MultiActiveReqType extends Enumeration {
  type MultiActiveReqType = Value
  val

  INVALID,

  ATTACH_OLD_SESSION,

  CREATE_SESSION,

  ASK_THRIFT_ADDR = Value
}
