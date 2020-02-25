package org.apache.livy.cluster

import org.apache.livy.server.recovery.{StateStore, ZooKeeperManager}
import org.apache.livy.sessions.Session.RecoveryMetadata
import org.apache.livy.sessions.{Session, SessionManager}
import org.apache.livy.LivyConf

import scala.reflect.ClassTag

class ServerSessionMapping[S <: Session, R <: RecoveryMetadata : ClassTag](
    livyConf: LivyConf,
    clusterManager: ClusterManager,
    sessionType: String,
    sessionManager: SessionManager[S, R],
    stateStore: StateStore,
    zooKeeperManager: ZooKeeperManager) extends SessionAllocator[S, R] {

  private val basePath = livyConf.get(LivyConf.HA_ALLOCATOR_MAPPING)

  override def findServer(sessionId: Int): Option[ServiceNode] = {
    if (sessionManager) {

    }
  }

  override def allocateServer(sessionId: Int): ServiceNode = {
    zooKeeperManager
  }

  override def onServerJoin(server: ServiceNode): Unit = {}

  override def onServerLeave(server: ServiceNode): Unit = {}

  private def find(sessionId: Int): Option[ServiceNode] = {
    stateStore.get[ServiceNode](s"$basePath/$sessionType/$sessionId")
  }

  private def put(sessionId: Int, server: ServiceNode): Unit = {
    stateStore.set(s"$basePath/$sessionType/$sessionId", server)
  }

  private def chooseServer(): ServiceNode = {
  }
}
