package akka.persistence.common

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import scalikejdbc.async.{AsyncConnectionPool, AsyncConnectionPoolSettings}

private[persistence] object ScalikeJDBCExtension extends ExtensionId[ScalikeJDBCExtension] with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem): ScalikeJDBCExtension = {
    new ScalikeJDBCExtension(system)
  }

  override def lookup(): ExtensionId[_ <: Extension] = ScalikeJDBCExtension
}

private[persistence] class ScalikeJDBCExtension(system: ExtendedActorSystem) extends Extension {
  val config = SQLAsyncConfig(system)
  val connectionPoolName = s"${config.rootKey}.${config.url}"
  val sessionProvider: ScalikeJDBCSessionProvider = {
    new DefaultScalikeJDBCSessionProvider(connectionPoolName, system.dispatcher)
  }

  def initialize(passwordProvider: () => String = () => ""): Unit = {
    val providedPassword = passwordProvider()
    val password = if (providedPassword == "") config.password else providedPassword

    AsyncConnectionPool.add(
      name = connectionPoolName,
      url = config.url,
      user = config.user,
      password = if (password == "") null else password,
      settings = AsyncConnectionPoolSettings(
        maxPoolSize = config.maxPoolSize,
        maxQueueSize = config.waitQueueCapacity)
    )
  }
}
