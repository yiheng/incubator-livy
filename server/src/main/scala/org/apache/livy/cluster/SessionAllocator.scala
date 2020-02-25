package org.apache.livy.cluster

import org.apache.livy.sessions.Session
import org.apache.livy.sessions.Session.RecoveryMetadata

import scala.reflect.ClassTag

abstract class SessionAllocator[S <: Session, R <: RecoveryMetadata : ClassTag] {
  def findServer(sessionId: Int): Option[ServiceNode]

  def allocateServer(sessionId: Int): ServiceNode

  def onServerJoin(server: ServiceNode): Unit

  def onServerLeave(server: ServiceNode): Unit
}
