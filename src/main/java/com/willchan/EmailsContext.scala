package com.willchan

import java.io.Serializable
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Minutes}

import scala.collection.immutable.IndexedSeq

class EmailsContext extends Serializable {

  @transient
  private final var sc: SparkContext = null

  def this(sc: SparkContext) {
    this()
    this.sc = sc
  }

  /**
    * @return RDD of EmailLinks parsed in the paths. Only .txt files are parsed.
    */
  def emailsRDD(paths: String): RDD[EmailLink] = {
    val files: RDD[(String, String)] = sc.wholeTextFiles(paths, 4)
    files.filter(_._1.endsWith(".txt")).flatMap { case (_, content) =>
      EmailLink.parse(content)
    }
  }

  /**
    * @return an RDD of email to tuples of date and number of emails received on that date
    */
  def emailsReceivedPerPersonPerDay(paths: String): RDD[(String, Iterable[(Date, Int)])] = {
    val byPerson = _emailsReceivedPerPersonPerDay(paths).groupBy(_._1._1)
    byPerson.map { case (email, results) =>
      val tuples = for (res <- results) yield {
        (res._1._2, res._2)
      }
      (email, tuples)
    }
  }

  private def _emailsReceivedPerPersonPerDay(paths: String): RDD[((String, Date), Int)] = {
    // EmailLink -> (to, normalized date), then count
    val emailsRdd: RDD[EmailLink] = emailsRDD(paths).cache()
    val recipientsAndDates: RDD[((String, Date), Int)] = emailsRdd.map { case email =>
      ((email.to, new DateTime(email.date).withTimeAtStartOfDay().toDate), 1)
    }
    recipientsAndDates.reduceByKey((a, b) => a + b)
  }

  /**
    * @return the email address removing the most direct emails
    */
  def personsReceivingMostDirectEmails(paths: String): (Int, Iterable[String]) = {
    val emailsRdd: RDD[EmailLink] = emailsRDD(paths).cache()
    val directEmails: RDD[EmailLink] = emailsRdd.filter(_.numRecipients == 1)
    val recipientAndNumReceived: RDD[(String, Int)] = directEmails.map(email => (email.to, 1)).reduceByKey((a, b) => a + b)
    val numReceivedAndRecipient: RDD[(Int, String)] = recipientAndNumReceived.map { case (recipient, count) => (count, recipient) }
    val sorted: RDD[(Int, Iterable[String])] = numReceivedAndRecipient.groupByKey().sortBy(_._1, ascending = false)
    sorted.first()
  }

  /**
    * @return the email address that sent the most broadcastemails
    */
  def personsSendingMostBroadcastEmails(paths: String): (Int, Iterable[String]) = {
    val emailsRdd: RDD[EmailLink] = emailsRDD(paths).cache()
    val broadcastEmails: RDD[EmailLink] = emailsRdd.filter(_.numRecipients > 1)
    val senderAndNumSent: RDD[(String, Int)] = broadcastEmails.map(email => (email.from, 1)).reduceByKey((a, b) => a + b)
    val numSentAndSender: RDD[(Int, String)] = senderAndNumSent.map { case (recipient, count) => (count, recipient) }
    val sorted: RDD[(Int, Iterable[String])] = numSentAndSender.groupByKey().sortBy(_._1, ascending = false)
    sorted.first()
  }

  /**
    * @param minutes maximum number of minutes that qualifies a reply as quick
    * @return pairs of EmailLinks, the second of the which is a quick response to the first
    */
  def emailsRespondedToWithin(paths: String, minutes: Int): RDD[(EmailLink, EmailLink)] = {
    val emailsRdd: RDD[EmailLink] = emailsRDD(paths).cache()
    val notEmailsToSelf: RDD[EmailLink] = emailsRdd.filter(email => email.to != email.from).cache()
    val emailsByFromTo: RDD[((String, String), EmailLink)] = notEmailsToSelf.keyBy(email => (email.from, email.to))
    val emailsByToFrom: RDD[((String, String), EmailLink)] = notEmailsToSelf.keyBy(email => (email.to, email.from))

    val candidateReplyPairs: RDD[((EmailLink, EmailLink))] = emailsByFromTo.join(emailsByToFrom).map(_._2)
    candidateReplyPairs.filter { case (initialEmail, reply) =>
      val date0 = new DateTime(initialEmail.date)
      val date1 = new DateTime(reply.date)
      date0.isBefore(date1) && Minutes.minutesBetween(date0, date1).getMinutes <= minutes && reply.subject.contains(initialEmail.subject)
    }
  }
}

object EmailsContext {

  private val resourcesDirRoot = "src/main/resources/enron_with_categories/"
  val resourcesDirPaths = {
    val paths: IndexedSeq[String] = for (i <- 1 to 8) yield resourcesDirRoot + i
    paths.mkString(",")
  }
  //  val quickPath = resourcesDirRoot + "2" // not many files in this directory

  def main(args: Array[String]) {
    val sc: SparkContext = new SparkContext("local[4]", "Enron data")
    val emailsContext: EmailsContext = new EmailsContext(sc)
    val testpath = resourcesDirPaths

    val startTime = System.currentTimeMillis()
    val q1 = emailsContext.emailsReceivedPerPersonPerDay(testpath).collect()
    val q2a: (Int, Iterable[String]) = emailsContext.personsReceivingMostDirectEmails(testpath)
    val q2b: (Int, Iterable[String]) = emailsContext.personsSendingMostBroadcastEmails(testpath)
    val q3: Array[(EmailLink, EmailLink)] = emailsContext.emailsRespondedToWithin(testpath, 10).collect()
    println(s"Perf: ${(System.currentTimeMillis() - startTime)/1000} seconds elapsed")
  }
}
