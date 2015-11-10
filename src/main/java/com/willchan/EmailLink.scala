package com.willchan

import java.io.ByteArrayInputStream
import java.util.{Date, Properties}
import javax.mail.internet.{MailDateFormat, MimeMessage}
import javax.mail.{Header, Session}

import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

case class EmailLink(messageId: String,
                     date: Date,
                     from: String,
                     to: String,
                     subject: String,
                     numRecipients: Int) extends Serializable

object EmailLink {

  val logger = LoggerFactory.getLogger(classOf[EmailLink])

  val messageIdPrefix = "Message-ID"
  val datePrefix = "Date"
  val fromPrefix = "From"
  val toPrefix = "To"
  val ccPrefix = "Cc"
  val bccPrefix = "Bcc"
  val subjectPrefix = "Subject"

  lazy val props = new Properties()
  lazy val dateFormat = new MailDateFormat()

  def parse(emailStr: String): List[EmailLink] = {
    var messageId: Option[String] = None
    var date: Option[Date] = None
    var sender: Option[String] = None
    val recipients: java.util.Set[String] = new java.util.HashSet[String]()
    var subject: Option[String] = None

    val is = new ByteArrayInputStream(emailStr.getBytes())
    val message: MimeMessage = new MimeMessage(Session.getDefaultInstance(props), is)
    val allHeaders = message.getAllHeaders
    while (allHeaders.hasMoreElements) {
      val h = allHeaders.nextElement().asInstanceOf[Header]
      if (h.getName == messageIdPrefix) {
        messageId = Option(h.getValue)

      } else if (h.getName == datePrefix) {
        date = Option(dateFormat.parse(h.getValue))

      } else if (h.getName == fromPrefix) {
        sender = Option(h.getValue)

      } else if (h.getName == toPrefix || h.getName == ccPrefix || h.getName == bccPrefix) {
        h.getValue.split(",").foreach(e => recipients.add(e.trim))

      } else if (h.getName == subjectPrefix) {
        subject = Option(h.getValue)
      }
    }

    if (messageId.isEmpty || date.isEmpty || sender.isEmpty || recipients.isEmpty || subject.isEmpty) {
      logger.debug("Error parsing email that begins: " + emailStr.take(50))
      List.empty[EmailLink]

    } else {
      recipients.asScala.map { to =>
        EmailLink(messageId.get, date.get, sender.get, to, subject.get, recipients.size)
      }.toList
    }
  }
}
