package mindriot.aws.stream

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.{ClosedShape, ActorAttributes}
import akka.stream.scaladsl._
import mindriot.aws.sqs.{Reader, Queue, Message, AttributeType}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.{Failure, Try}

/**
  * Created by admin on 2/18/16.
  */
object BroadcastQueueListener {

  def apply[T](maxMessages: Int,
               wait: Option[Int] = None,
               timeout: Option[Int] = None,
               withAttributes: Option[Seq[AttributeType]] = None,
               withCustomAttributes: Option[Seq[String]] = None,
               timeoutOffset: Int = 0)
              (workers: List[Message[T] => Unit])
              (implicit queue: Queue, system: ActorSystem, reader: Reader[T]): BroadcastQueueListener[T] =
    new BroadcastQueueListener[T](maxMessages, wait, timeout, withAttributes, withCustomAttributes, timeoutOffset)(workers)(queue, system, reader)

}


class BroadcastQueueListener[T](maxMessages: Int,
                                wait: Option[Int] = None,
                                timeout: Option[Int] = None,
                                withAttributes: Option[Seq[AttributeType]] = None,
                                withCustomAttributes: Option[Seq[String]] = None,
                                timeoutOffset: Int = 0)
                               (workers: List[Message[T] => Unit])
                               (implicit queue: Queue, system: ActorSystem, reader: Reader[T])
  extends QueueListener[T](maxMessages, wait, timeout, withAttributes, withCustomAttributes, timeoutOffset)(queue, system, reader) {

  override val log = LoggerFactory.getLogger(this.getClass)

  override def inputBufferSize = 1
  log.info(s"Setting input buffer size to $inputBufferSize")

  protected val workerCount = workers.length

  protected def sink: List[Sink[Message[T], Future[Unit]]] = {
    workers.map { workUnit =>
      Sink.foreach[Message[T]] { msg =>
        Try(workUnit(msg)) match {
          case Failure(e) => {
            log.error( s"""Unhandled queue worker failure, queue: '${queue.url}',
                        error: ${e.getMessage}.Stack Trace: ${e.printStackTrace}""".stripMargin)
            throw e
          }
          case _ =>
        }
      }
    }
  }


  protected def graph[U](source: Source[Message[T], ActorRef],
                         sink: List[Sink[Message[T], U]]): RunnableGraph[Unit] =

    RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      log.debug(s"Creating broadcast RunnableGraph with $workerCount workers...")

      val bcast = builder.add(Broadcast[Message[T]](workerCount))
      source ~> bcast.in
      for(i <- 0 until workerCount) bcast.out(i) ~> sink(i)
      ClosedShape

    }.named("QueueWorkerBalanceFlowGraph")
      .withAttributes(ActorAttributes.supervisionStrategy(decider)))
  /**
    * Run the flow
    */
  override lazy val run = graph(source, sink) run
}
