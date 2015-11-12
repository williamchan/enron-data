package com.willchan

import java.io.ByteArrayInputStream
import java.util.{Date, Properties}
import javax.mail.internet.{MailDateFormat, MimeMessage}
import javax.mail.{Header, Session}

import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Represents a bidirectional messages between a sender and recipient, though the email may have multiple recipients.
  * From such an email, multiple EmailLinks would be created.
  */
case class EmailLink(messageId: String,
                     date: Date,
                     from: String,
                     to: String,
                     subject: String,
                     numRecipients: Int) extends Serializable

object EmailLink {

  val logger = LoggerFactory.getLogger(classOf[EmailLink])

  val messageIdHeader = "Message-ID"
  val dateHeader = "Date"
  val fromHeader = "From"
  val toHeader = "To"
  val ccHeader = "Cc"
  val bccHeader = "Bcc"
  val subjectHeader = "Subject"

  lazy val props = new Properties()
  lazy val dateFormat = new MailDateFormat()

  /**
    * @return a list of EmailLinks that were successfully parsed, excluding those emails that were lacking a Message-ID,
    *         Date, From, To, or Subject.
    */
  def parse(emailStr: String): List[EmailLink] = {
    var messageId: Option[String] = None
    var date: Option[Date] = None
    var sender: Option[String] = None
    val recipients: java.util.Set[String] = new java.util.HashSet[String]()
    var subject: Option[String] = None

    val is = new ByteArrayInputStream(emailStr.getBytes)
    val message: MimeMessage = new MimeMessage(Session.getDefaultInstance(props), is)
    val allHeaders = message.getAllHeaders
    while (allHeaders.hasMoreElements) {
      val h = allHeaders.nextElement().asInstanceOf[Header]
      if (h.getName == messageIdHeader) {
        messageId = Option(h.getValue)

      } else if (h.getName == dateHeader) {
        date = Option(dateFormat.parse(h.getValue))

      } else if (h.getName == fromHeader) {
        sender = Option(h.getValue)

      } else if (h.getName == toHeader || h.getName == ccHeader || h.getName == bccHeader) {
        h.getValue.split(",").foreach(e => recipients.add(e.trim))

      } else if (h.getName == subjectHeader) {
        subject = Option(h.getValue)
      }
    }

    if (messageId.isEmpty || date.isEmpty || sender.isEmpty || recipients.isEmpty || subject.isEmpty) {
      logger.info("Error parsing email that begins: " + emailStr.take(50))
      List.empty[EmailLink]

    } else {
      recipients.asScala.map { to =>
        EmailLink(messageId.get, date.get, sender.get, to, subject.get, recipients.size)
      }.toList
    }
  }
}
