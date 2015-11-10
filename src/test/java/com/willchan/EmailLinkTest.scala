package com.willchan

import java.io.{File, FileInputStream}
import javax.mail.internet.MailDateFormat

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class EmailLinkTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  "parse" should "return email records" in {
    val emailStr = getStringFromFile(new File("src/main/resources/enron_with_categories/2/176571.txt"))
    val parsed: List[EmailLink] = EmailLink.parse(emailStr)
    parsed.foreach{case email => email.messageId should equal("<29137643.1075849869791.JavaMail.evans@thyme>")}
    parsed.foreach{case email => email.date should equal(new MailDateFormat().parse("Sun, 8 Jul 2001 05:40:00 -0700 (PDT)"))}
    parsed.foreach{case email => email.from should equal("steven.kean@enron.com")}
    parsed.foreach{case email => email.subject should equal("Re: FW: Dadisms")}
    parsed.foreach{case email => email.numRecipients should equal(8)}
    parsed.map(_.to).toSet should equal(Set("rkean@starband.net", "rex04@msn.com", "dkreiman@mcleodusa.net",
      "kat.wedig@netzero.net", "kean@rice.edu", "kean.philip@mcleodusa.net", "skean@enron.com", "dkean@starband.net"))
  }

  private def getStringFromFile(file: File): String = {
    val inputStream: FileInputStream = new FileInputStream(file)
    val bytes: Array[Byte] = new Array[Byte](inputStream.available)
    inputStream.read(bytes)
    new String(bytes)
  }
}
