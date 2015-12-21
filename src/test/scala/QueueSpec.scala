
import com.amazonaws.auth.{BasicAWSCredentials, AWSCredentials}
import com.amazonaws.regions.Regions
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

  val config = ConfigFactory.load()
  val accessKey = config.getString("aws.accessKey")
  val secretKey = config.getString("aws.secretKey")
  val region = config.getString("aws.region")

  val awsCredentials: AWSCredentials = new BasicAWSCredentials(accessKey, secretKey)
  implicit val cn = Connection(awsCredentials, Regions.fromName(region))

  val r = new Random()

  val timeout = 5 seconds
  val prefix = s"${randomString(5)}-XYZ-"
  val queueName1 = s"${prefix}${randomString(5)}"
  val queueName2 =  s"${prefix}${randomString(5)}"

  val msg = "Sample string message"


  def beforeAll = {
    println(s"Queue1: '$queueName1")
    println(s"Queue2: '$queueName2")
  }

  def afterAll = {
    cn close
  }

  def randomString(length: Int) = r.alphanumeric.take(length).mkString


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
