
import com.amazonaws.auth.{BasicAWSCredentials, AWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.model.MessageAttributeValue
import com.typesafe.config.ConfigFactory
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterAll
import org.specs2.concurrent.ExecutionEnv


import scala.concurrent.Await
import scala.concurrent.duration._

import mindriot.aws.sqs._
import StringFormat._
import scala.util.Random


class QueueSpec extends Specification with BeforeAfterAll {

  sequential

  /**
    * Sample application.conf configuration
    * aws.accessKey = "TEST_ACCESS_KEY"
    * aws.secretKey = "TEST_SECRET_KEY"
    * aws.region = "us-west-2"
    * aws.account = "TEST_ACCOUNT_NUMBER"
    */
  val config = ConfigFactory.load()
  val accessKey = config.getString("aws.accessKey")
  val secretKey = config.getString("aws.secretKey")
  val region = config.getString("aws.region")
  val accountNum = config.getString("aws.account")

  val awsCredentials: AWSCredentials = new BasicAWSCredentials(accessKey, secretKey)
  implicit val cn = Connection(awsCredentials, Regions.fromName(region))

  val r = new Random()

  val timeout = 10 seconds
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
      Queue.create(queueName1).map(_.url.length  must beGreaterThan(5)).await(0, timeout)
      Queue.create(queueName2).map(_.url.length  must beGreaterThan(5)).await(0, timeout)
    }

    "list queues" in { implicit ee: ExecutionEnv =>
      Queue.list(prefix).map(_.length) must be_==(2).await(0, timeout)
    }


    "list queue attributes" in { implicit ee: ExecutionEnv =>
      val queue = Await.result(Queue.list(queueName1).map(_.head), 2 seconds)

      queue.attributes().map{ attr =>
        attr(QueueAttributeType.VisibilityTimeout) must be_==("30")
        attr(QueueAttributeType.MaximumMessageSize) must be_==("262144")
        attr(QueueAttributeType.ReceiveMessageWaitTimeSeconds) must be_==("0")
        attr(QueueAttributeType.MessageRetentionPeriod) must be_==("345600")
      }.await(0, timeout)

    }


    "send message with attributes" in { implicit ee: ExecutionEnv =>
      val queue = Await.result(Queue.list(queueName1).map(_.head), 2 seconds)
      queue.send(msg, Map("One" -> new MessageAttributeValue().withDataType("String").withStringValue("1"))) must be_==(()).await(0, timeout)
    }


    "receive message with attributes" in { implicit ee: ExecutionEnv =>
      val queue = Await.result(Queue.list(queueName1).map(_.head), 2 seconds)
      queue.receive(withAttributes = Some(Seq(MessageAttributeType.ApproximateReceiveCount)),
                    withCustomAttributes = Some(Seq("One"))).map { lst =>
        val m = lst.head
        m.nack
        m.body must be_==(msg)
        m.messageAttributes("One").getStringValue must equalTo("1")
        m.attributes(MessageAttributeType.ApproximateReceiveCount) must be_==("1")
      }.await(0, timeout)
    }


    "purge queue" in { implicit ee: ExecutionEnv =>
      val queue = Await.result(Queue.list(queueName2).map(_.head), 2 seconds)
      for( i <- 0 to 2 ) yield Await.result(queue.send(msg + s"-$i"), 2 seconds)
      queue.purge() must be_==(()).await(0, timeout)

    }


    "add/remove permissions" in { implicit ee: ExecutionEnv =>
      val queue = Await.result(Queue.list(queueName1).map(_.head), 2 seconds)

      // add send recieve and verify
      Await.result(queue.addPermission("all-sendReceive", Seq(accountNum), Seq(Permission.SendMessage, Permission.ReceiveMessage)), 2 seconds)
      queue.attributes(QueueAttributeType.Policy).map { attr =>
        attr(QueueAttributeType.Policy) must contain(""""Action":["SQS:ReceiveMessage","SQS:SendMessage"]""")
      }.await(0, timeout)

      // remove send recieve and verify
      Await.result(queue.removePermission("all-sendReceive"), 2 seconds)
      queue.attributes(QueueAttributeType.Policy).map { attr =>
        attr(QueueAttributeType.Policy) must contain(""""Statement":[]""")
      }.await(0, timeout)

      // add all and verify
      Await.result(queue.addPermission("all-permissions", Seq(accountNum)), 2 seconds)
      queue.attributes(QueueAttributeType.Policy).map { attr =>
        attr(QueueAttributeType.Policy) must contain(""""Action":"SQS:*"""")
      }.await(0, timeout)

    }

    "change visibility timeout" in { implicit ee: ExecutionEnv =>
      val queue = Await.result(Queue.list(queueName1).map(_.head), 2 seconds)

      Await.result(queue.send(msg), 2 seconds)
      val optMsg = Await.result(queue.receiveOne(timeout = Some(1)), 2 seconds)
      //change visibility time out to 30s, sleep through original timeout and check if message is visible.
      //(without change to visiblity timeout test case will fail)
      optMsg.foreach(queue.changeMessageVisibility(_, 30))
      Thread.sleep(2000)
      queue.receiveOne().map(_ must beNone).await(0, timeout)

    }

    "delete queues" in { implicit ee: ExecutionEnv =>
      Queue.list(prefix).map {
        _.map { q =>
          println(s"Deleting '${q.url}'")
          Queue.delete(q.url) must be_==(()).await(0, timeout)
        }
      } await(0, timeout)
    }

  }


}
