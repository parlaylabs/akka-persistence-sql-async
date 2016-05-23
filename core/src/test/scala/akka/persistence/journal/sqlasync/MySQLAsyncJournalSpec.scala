package akka.persistence.journal.sqlasync

import akka.persistence.CapabilityFlag
import akka.persistence.helper.MySQLInitializer
import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

class MySQLAsyncJournalSpec
  extends JournalSpec(ConfigFactory.load("mysql-application.conf"))
  with MySQLInitializer {

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.on()
}
