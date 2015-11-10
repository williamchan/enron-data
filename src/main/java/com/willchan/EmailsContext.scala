package com.willchan

import java.io.Serializable
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import SparkContext._
import org.joda.time.{Minutes, DateTime}
import scala.collection.JavaConversions._
import scala.collection.immutable.IndexedSeq

class EmailsContext extends Serializable {

  import EmailsContext._

  @transient
  private final var sc: SparkContext = null

  def this(sc: SparkContext) {
    this()
    this.sc = sc
  }

  def emailsRDD(paths: String): RDD[EmailLink] = {
    val files: RDD[(String, String)] = sc.wholeTextFiles(paths)
    files.filter(_._1.endsWith(".txt")).flatMap { case (_, content) =>
      EmailLink.parse(content)
    }
  }

  /**
    * EmailLink -> (to, normalized date), then count
    */
  def emailsReceivedPerPersonPerDay(paths: String): RDD[((String, Date), Int)] = {
    val recipientsAndDates: RDD[((String, Date), Int)] = emailsRDD(paths).map { case email =>
      ((email.to, new DateTime(email.date).withTimeAtStartOfDay().toDate), 1)
    }
    recipientsAndDates.reduceByKey((a, b) => a + b)
  }

  def personsReceivingMostDirectEmails(paths: String): (Int, Iterable[String]) = {
    val directEmails: RDD[EmailLink] = emailsRDD(paths).filter(_.numRecipients == 1)
    val recipientAndNumReceived: RDD[(String, Int)] = directEmails.map(email => (email.to, 1)).reduceByKey((a, b) => a + b)
    val numReceivedAndRecipient: RDD[(Int, String)] = recipientAndNumReceived.map { case (recipient, count) => (count, recipient) }
    val sorted: RDD[(Int, Iterable[String])] = numReceivedAndRecipient.groupByKey().sortBy(_._1, ascending = false)
    sorted.first()
  }

  def personsSendingMostBroadcastEmails(paths: String): (Int, Iterable[String]) = {
    val broadcastEmails: RDD[EmailLink] = emailsRDD(paths).filter(_.numRecipients > 1)
    val senderAndNumSent: RDD[(String, Int)] = broadcastEmails.map(email => (email.from, 1)).reduceByKey((a, b) => a + b)
    val numSentAndSender: RDD[(Int, String)] = senderAndNumSent.map { case (recipient, count) => (count, recipient) }
    val sorted: RDD[(Int, Iterable[String])] = numSentAndSender.groupByKey().sortBy(_._1, ascending = false)
    sorted.first()
  }

  def emailsRespondedToWithinMinute(paths: String): RDD[(EmailLink, EmailLink)] = {
    val notEmailsToSelf: RDD[EmailLink] = emailsRDD(paths).filter(email => email.to != email.from)
    val emailsByFromTo: RDD[((String, String), EmailLink)] = notEmailsToSelf.keyBy(email => (email.from, email.to))
    val emailsByToFrom: RDD[((String, String), EmailLink)] = notEmailsToSelf.keyBy(email => (email.to, email.from))

    val candidateReplyPairs: RDD[((EmailLink, EmailLink))] = emailsByFromTo.join(emailsByToFrom).map(_._2)
    candidateReplyPairs.filter { case (initialEmail, reply) =>
      val date0 = new DateTime(initialEmail.date)
      val date1 = new DateTime(reply.date)
      date0.isBefore(date1) && Minutes.minutesBetween(date0, date1).getMinutes <= 1 && reply.subject.contains(initialEmail.subject)
    }
  }
}

object EmailsContext {

  private val resourcesDirRoot = "src/main/resources/enron_with_categories/"
  val resourcesDirPaths = {
    val paths: IndexedSeq[String] = for (i <- 6 to 6) yield resourcesDirRoot + i
    paths.mkString(",")
  }
  val quickPath = resourcesDirRoot + "2"

  def main(args: Array[String]) {
    val sc: SparkContext = new SparkContext("local[4]", "Enron data")
    val emailsContext: EmailsContext = new EmailsContext(sc)
    val testpath = resourcesDirPaths

    //    val q1 = emailsContext.emailsReceivedPerPersonPerDay(testpath).collect()

    //    val q2a: (Int, Iterable[String]) = emailsContext.personsReceivingMostDirectEmails(testpath)

//    val q2b: (Int, Iterable[String]) = emailsContext.personsSendingMostBroadcastEmails(testpath)

    val q3: Array[(EmailLink, EmailLink)] = emailsContext.emailsRespondedToWithinMinute(testpath).collect()
    q3
  }
}
