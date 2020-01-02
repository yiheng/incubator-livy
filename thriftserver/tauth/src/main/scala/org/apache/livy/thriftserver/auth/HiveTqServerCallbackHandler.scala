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

package org.apache.livy.thriftserver.auth

import com.tencent.tdw.security.Tuple
import com.tencent.tdw.security.authentication.tauth.AuthServer
import com.tencent.tdw.security.netbeans.AuthTicket
import org.apache.hadoop.security.{AuthConfigureHolder, Utils}
import org.apache.hadoop.security.sasl.{TqServerCallbackHandler, TqTicketResponseToken}

class  HiveTqServerCallbackHandler(serviceName: String)
  extends TqServerCallbackHandler {

  override def processResponse(response: Array[Byte]): Tuple[Array[Byte], String] = {
    val token = TqTicketResponseToken.valueOf(response)
    val authServer = AuthServer.getDefault
    val ret = authServer.auth(new AuthTicket(
      token.getSmkId,
      token.getServiceTicket,
      token.getAuthenticator))
    Tuple.of(ret.getTicket.getSessionKey, ret.getAuthenticator.getPrinciple)
  }

  override def getServiceName: String = {
    if (serviceName != null) {
      serviceName
    } else {
      Utils.getSystemPropertyOrEnvVar("tq.service.name")
    }
  }

  override def isNeedAuth(extraId: Array[Byte]): Boolean = AuthConfigureHolder.isAuthEnable

  override def isForbidden(userName: String): Boolean = AuthConfigureHolder.isNotAllow(userName)

}
