
import com.typesafe.config.ConfigFactory
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll
import org.specs2.concurrent.ExecutionEnv


import scala.concurrent.Await
import scala.concurrent.duration._

import mindriot.aws.sqs._
import StringFormatter._
import scala.util.Random


class QueueSpec extends Specification with BeforeAfterAll {

  sequential

  implicit val cn = Connection()

  val r = new Random()

  val timeout = 5 seconds
  val prefix = s"${r.alphanumeric.take(5).mkString}-XYZ-"
  val queueName1 = s"${prefix}${r.alphanumeric.take(5).mkString}"
  val queueName2 =  s"${prefix}${r.alphanumeric.take(5).mkString}"

  val msg = "Sample string message"


  def beforeAll = {
    println(s"Queue1: '$queueName1")
    println(s"Queue2: '$queueName2")
  }

  def afterAll = {
    println("******** AFTER-ALL *********")
    cn close
  }


  "Queue" should {


    "create queues" in { implicit ee: ExecutionEnv =>
      //Queue.create(queueName1).map(_.url.length  must beGreaterThan(5)) await(0, timeout)
      //Queue.create(queueName2).map(_.url.length  must beGreaterThan(5)) await(0, timeout)

      Queue.create(queueName1)
      Queue.create(queueName2)
      Thread.sleep(2000)

      //assume success, if failure all follow up tests will fail.
      success

    }

    "list queues" in { implicit ee: ExecutionEnv =>
      Queue.list(prefix).map(_.length) must be_==(2).await(0, timeout)
    }

    "send message" in { implicit ee: ExecutionEnv =>
      val queue = Await.result(Queue.list(queueName1).map(_.head), 2 seconds)
      queue.send(msg) must be_==(()).await(0, timeout)
    }

    "receive message" in { implicit ee: ExecutionEnv =>
      val queue = Await.result(Queue.list(queueName1).map(_.head), 2 seconds)
      queue.receive().map { lst => lst.head.body must be_==(msg) }.await(0, timeout)
    }

    "delete queues" in { implicit ee: ExecutionEnv =>
      Queue.list(prefix).map{
        _.map { q =>
          println(s"Deleting '${q.url}'")
          Queue.delete(q.url) must be_==(()).await(0, timeout)
        }
      } await
    }
  }


}
