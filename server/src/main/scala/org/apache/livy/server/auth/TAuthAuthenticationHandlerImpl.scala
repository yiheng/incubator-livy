package org.apache.livy.server.auth

import java.io.IOException
import java.util.Properties

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import javax.servlet.ServletException
import org.apache.hadoop.security.authentication.client.AuthenticationException
import org.apache.hadoop.security.authentication.server.{AuthenticationHandler, AuthenticationToken}
import org.apache.livy.Logging
import com.tencent.tdw.security.authentication.{Authentication, LocalKeyManager}
import com.tencent.tdw.security.authentication.service.{SecureService, SecureServiceFactory}

class TAuthAuthenticationHandlerImpl extends AuthenticationHandler with Logging {
  private var secureService: SecureService = _

  override def getType: String = TAuthAuthenticationHandlerImpl.TYPE

  @throws[ServletException]
  override def init(config: Properties): Unit = {
    Option(config.getProperty(TAuthAuthenticationHandlerImpl.TDW_SECURITY_URL)).foreach(url =>
      System.setProperty(TAuthAuthenticationHandlerImpl.TDW_SECURITY_URL, url)
    )

    System.setProperty(TAuthAuthenticationHandlerImpl.ENABLE_ANTI_REPLAY,
      config.getProperty(TAuthAuthenticationHandlerImpl.ENABLE_ANTI_REPLAY))

    val keyPath = config.getProperty(TAuthAuthenticationHandlerImpl.KEY_PATH)
    val antiReplay = config.getProperty(TAuthAuthenticationHandlerImpl.ENABLE_ANTI_REPLAY)
      .toBoolean
    val serviceTarget = config.getProperty(TAuthAuthenticationHandlerImpl.SERVICE_TARGET)
    this.secureService = SecureServiceFactory.getOrCreate(
      new SecureServiceFactory.ServiceConf(serviceTarget, null, antiReplay,
        LocalKeyManager.generateByDir(keyPath, true)))
  }

  override def destroy(): Unit = { }

  @throws[IOException]
  @throws[AuthenticationException]
  override def managementOperation(
      token: AuthenticationToken,
      request: HttpServletRequest,
      response: HttpServletResponse): Boolean = true

  override def authenticate(
      request: HttpServletRequest,
      response: HttpServletResponse): AuthenticationToken = {
    var token: AuthenticationToken = null
    val authentication = request.getHeader("secure-authentication")


    if (authentication != null) {
      try {
        val authenticator = secureService.authenticate(Authentication.valueOf(authentication))
        token = new AuthenticationToken(authenticator.getUser, authenticator.getUser,
          TAuthAuthenticationHandlerImpl.TYPE)
      } catch {
        case e: Exception => {
          e.printStackTrace()
          logger.warn("Cannot verify authentication token " + authentication)
        }
      }
    } else {
      response.setHeader("WWW-Authenticate", "Basic")
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
      trace("Basic auth starting")
    }
    token
  }
}

object TAuthAuthenticationHandlerImpl {
  val TYPE = "tauth"
  val TDW_SECURITY_URL = "tdw.security.url"
  val ENABLE_ANTI_REPLAY = "security.authentication.anti-replay.enable"
  val KEY_PATH = "security.authentication.key-path"
  val SERVICE_TARGET = "service.target"
}