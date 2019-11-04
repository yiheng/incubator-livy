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

package org.apache.livy.utils

import java.security.MessageDigest
import java.util.TreeMap

import scala.collection.mutable.Set

/**
  * For Consistent Hash Algorithm, See:
  * https://www.cs.princeton.edu/courses/archive/fall09/cos518/papers/chash.pdf
  */
class ConsistentHash(replicateNum: Int) {
  private val MD5_BYTE_LEN = 16
  private val HASH_BYTE_LEN = 4
  private val REPLICATE_NUM_SHARE_MD5 = MD5_BYTE_LEN / HASH_BYTE_LEN

  private val virtualNodes = new TreeMap[Long, String]()

  def this(nodes: List[String], replicateNum: Int) {
    this(replicateNum)
    for (node <- nodes) {
      addNode(node)
    }
  }

  def addNode(node: String): Unit = {
    // one digest is 16 bytes, each replicate get hash value from 4 bytes of digest
    // so 4 replicates, i.e.MD5_HAS_HASH_NUM, share one digest
    val md5Num = replicateNum / REPLICATE_NUM_SHARE_MD5
    for (i <- 0 until md5Num) {
      val digest = md5(node + i)
      for (k <- 0 until REPLICATE_NUM_SHARE_MD5) {
        val key = hash(digest, k)
        virtualNodes.put(key, node)
      }
    }

    val modReplicateNum = replicateNum % REPLICATE_NUM_SHARE_MD5
    if (modReplicateNum != 0) {
      val digest = md5(node + md5Num)
      for (k <- 0 until modReplicateNum) {
        val key = hash(digest, k)
        virtualNodes.put(key, node)
      }
    }
  }

  private def md5(input: String): Array[Byte] = {
    val md5 = MessageDigest.getInstance("MD5")
    md5.reset()

    val bytes = input.getBytes("UTF-8")
    md5.update(bytes)

    return md5.digest()
  }

  private def hash(digest: Array[Byte], number: Int): Long = {
    // get hash value from 4, i.e.HASH_BYTE_LEN, bytes of digest
    // if num is 1, get the 4, 5, 6, 7 byte of digest
    ((((digest(3 + number * 4) & 0xFF) << 24)) |
      (((digest(2 + number * 4) & 0xFF) << 16)) |
      (((digest(1 + number * 4) & 0xFF) << 8)) |
      (digest(0 + number * 4) & 0xFF)) &
      0xFFFFFFFFL
  }

  def removeNode(node: String): Unit = {
    val it = virtualNodes.entrySet().iterator()
    while (it.hasNext) {
      val entry = it.next()
      if (entry.getValue == node) {
        it.remove()
      }
    }
  }

  /**
    * @param key The key to hash
    * @return The nearest node to the hash(key)
    */
  def searchNode(key: String): Option[String] = {
    if (virtualNodes.isEmpty) {
      return None
    }

    val digest = md5(key)
    val hashValue = hash(digest, 0)

    val it = virtualNodes.entrySet().iterator()
    while (it.hasNext) {
      val entry = it.next()
      if (entry.getKey >= hashValue) {
        return Some(entry.getValue)
      }
    }

    Some(virtualNodes.firstEntry().getValue)
  }

  def getNodes(): Set[String] = {
    val nodes: Set[String] = Set()
    val it = virtualNodes.entrySet().iterator()
    while(it.hasNext) {
      val entry = it.next()
      nodes.add(entry.getValue)
    }

    nodes
  }
}
