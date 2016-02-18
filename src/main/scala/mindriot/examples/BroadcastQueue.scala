package mindriot.examples

import akka.actor.ActorSystem
import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.typesafe.config.ConfigFactory
import mindriot.aws.sqs.StringFormat._
import mindriot.aws.sqs.{Connection, Message, Queue}
import mindriot.aws.stream.BroadcastQueueListener

import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.StdIn
import scala.util.Random

object BroadcastQueue {

  val config = ConfigFactory.load()
  val accessKey = config.getString("aws.accessKey")
  val secretKey = config.getString("aws.secretKey")
  val region = config.getString("aws.region")

  val creds: AWSCredentials = new BasicAWSCredentials(accessKey, secretKey)
  implicit val cn = Connection(creds, Regions.fromName(region))

  def randomString(length: Int) = Random.alphanumeric.take(length).mkString

  val prefix = s"${randomString(5)}-ZZZ-"
  val queueName = s"${prefix}${randomString(5)}"

  implicit val system = ActorSystem("broadcast-queue-listener")


  def processAck(id: Int, msg: Message[String]): Unit = msg.ack

  def processMessage(id: Int, msg: Message[String]): Unit = {
    msg.ack
    Thread.sleep(1000)
    println(s"Worker $id received message with body: ${msg.body}")
  }

  val workers = List(processAck(0, _: Message[String])) ::: (0 to 3).map(n => processMessage(n, _: Message[String])).toList

  def main(args: Array[String]) = {

    println(s"Creating queue: $queueName")

    var url: String = ""

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
    val listener = BroadcastQueueListener.apply[String](1)(workers)
    listener run
  }
}
