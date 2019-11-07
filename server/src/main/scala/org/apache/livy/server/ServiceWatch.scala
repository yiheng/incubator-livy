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

import scala.collection.mutable.ArrayBuffer

import org.apache.livy.LivyConf
import org.apache.livy.LivyConf.SERVER_PORT
import org.apache.livy.Logging
import org.apache.livy.rsc.RSCConf.Entry.LAUNCHER_ADDRESS
import org.apache.livy.server.recovery.ZooKeeperManager
import org.apache.livy.utils.ConsistentHash

class ServiceWatch(livyConf: LivyConf, dir: String) extends Logging {
  require(ZooKeeperManager.get != null)
  private val consistentHash =
    new ConsistentHash(livyConf.getInt(LivyConf.HA_CONSISTENT_HASH_REPLICA_NUM))

  private val serverIP = livyConf.get(LAUNCHER_ADDRESS)
  require(serverIP != null, "Please config the livy.rsc.launcher.address")
  private val port = livyConf.getInt(SERVER_PORT)
  private val address = serverIP + ":" + port
  consistentHash.addNode(address)


  private val nodeAddListeners = new ArrayBuffer[String => Unit]()
  private val nodeRemoveListeners = new ArrayBuffer[String => Unit]()

  ZooKeeperManager.get.getChildren(dir).foreach(node => consistentHash.addNode(node))
  ZooKeeperManager.get.watchAddNode(dir, nodeAddHandler)
  ZooKeeperManager.get.watchRemoveNode(dir, nodeRemoveHandler)

  def register(): Unit = {
    ZooKeeperManager.get.createEphemeralNode(dir + "/" + address, address)
  }

  def contains(sessionId: Int): Boolean = {
    consistentHash.searchNode(sessionId.toString).getOrElse("") == address
  }

  def registerNodeAddListener(f : String => Unit): Unit = {
    nodeAddListeners.append(f)
  }

  def registerNodeRemoveListener(f : String => Unit): Unit = {
    nodeRemoveListeners.append(f)
  }

  private def nodeAddHandler(path: String, data: Array[Byte]): Unit = {
    val dataStr = (data.map(_.toChar)).mkString
    logger.info("Detect new node add: " + dataStr)
    consistentHash.addNode(dataStr)
    nodeAddListeners.foreach(_(dataStr))
  }

  private def nodeRemoveHandler(path: String, data: Array[Byte]): Unit = {
    val dataStr = (data.map(_.toChar)).mkString
    logger.info("Detect node removed: " + dataStr)
    consistentHash.removeNode(dataStr)
    nodeRemoveListeners.foreach(_(dataStr))
  }
}
