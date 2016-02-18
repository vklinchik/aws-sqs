package mindriot.examples

import akka.actor.ActorSystem
import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.typesafe.config.ConfigFactory
import mindriot.aws.sqs.StringFormat._
import mindriot.aws.sqs.{Connection, Message, Queue}
import mindriot.aws.stream.BalanceQueueListener

import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.StdIn
import scala.util.Random

object BalanceQueue {

  val config = ConfigFactory.load()
  val accessKey = config.getString("aws.accessKey")
  val secretKey = config.getString("aws.secretKey")
  val region = config.getString("aws.region")

  val creds: AWSCredentials = new BasicAWSCredentials(accessKey, secretKey)
  implicit val cn = Connection(creds, Regions.fromName(region))

  def randomString(length: Int) = Random.alphanumeric.take(length).mkString

  val prefix = s"${randomString(5)}-ZZZ-"
  val queueName = s"${prefix}${randomString(5)}"

  implicit val system = ActorSystem("balance-queue-listener")


  def processMessage(msg: Message[String]): Unit = {
    msg.ack
    Thread.sleep(1000)
    println(s"Worker received message with body: ${msg.body}")
  }

  def main(args: Array[String]) = {

    println(s"Creating queue: $queueName")

    var url = ""

    Queue.create(queueName) onSuccess {
      case q => {
        url = q.url
        println(s"Queue created, URL: ${q.url}")
        loadQueue(q, 10)
        configureQueueListener(q)
      }
    }

    Thread.sleep(10000)


    println("Hit any key to terminate.....")
    val input = StdIn.readLine
    println("exiting....")
    // cleanup and get out
    Queue.delete(url)
    Thread.sleep(1000)

    system.terminate
    System.exit(0)
  }

  def loadQueue(q: Queue, num: Int): Unit = {
    for(i <- 0 until num) q.send(s"Test message # - $i")
    Thread.sleep(num * 100)
  }


  def configureQueueListener(implicit q: Queue): Unit = {
    val listener = BalanceQueueListener.apply[String](10, 1)(processMessage)
    listener run
  }
}
