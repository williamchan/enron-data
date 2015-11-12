package com.willchan

import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}

class EmailsContextTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  val sc: SparkContext = new SparkContext("local[4]", "Enron data")
  val emailsContext: EmailsContext = new EmailsContext(sc)
  val testpath = EmailsContext.resourcesDirPaths

  "emailsReceivedPerPersonPerDay" should "run" in {
    val q1 = emailsContext.emailsReceivedPerPersonPerDay(testpath).collect()
    val index = q1.toMap
    index.size should equal(1314)
    index("jgreco@caithnessenergy.com").toList.length should equal(6)
  }

  "personsReceivingMostDirectEmails" should "return answer" in {
    val q2a: (Int, Iterable[String]) = emailsContext.personsReceivingMostDirectEmails(testpath)
    q2a._1 should equal(115)
    q2a._2.toList should equal(Iterable("maureen.mcvicker@enron.com"))
  }

  "personsSendingMostBroadcastEmails" should "run" in {
    val q2b: (Int, Iterable[String]) = emailsContext.personsSendingMostBroadcastEmails(testpath)
    q2b._1 should equal(1528)
    q2b._2.toList should equal(Iterable("miyung.buster@enron.com"))
  }

  "emailsRespondedToWithinMinute" should "run" in {
    val q3: Array[(EmailLink, EmailLink)] = emailsContext.emailsRespondedToWithin(testpath, 1).collect()
    q3.length should equal(0)
  }
}
