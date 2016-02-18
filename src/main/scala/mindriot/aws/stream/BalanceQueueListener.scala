package mindriot.aws.stream

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.{ClosedShape, ActorAttributes}
import akka.stream.scaladsl.{RunnableGraph, Balance, GraphDSL, Source, Sink}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.{Failure, Try}

import mindriot.aws.sqs.{Reader, AttributeType, Queue, Message}

object BalanceQueueListener {

  def apply[T](workerCount: Int,
               maxMessages: Int,
               wait: Option[Int] = None,
               timeout: Option[Int] = None,
               withAttributes: Option[Seq[AttributeType]] = None,
               withCustomAttributes: Option[Seq[String]] = None,
               timeoutOffset: Int = 0)
             (doWork: Message[T] => Unit)
             (implicit queue: Queue, system: ActorSystem, reader: Reader[T]): BalanceQueueListener[T] =
    new BalanceQueueListener[T](workerCount, maxMessages, wait, timeout, withAttributes, withCustomAttributes, timeoutOffset)(doWork)(queue, system, reader)
}




class BalanceQueueListener[T] (workerCount: Int,
                               maxMessages: Int,
                               wait: Option[Int],
                               timeout: Option[Int],
                               withAttributes: Option[Seq[AttributeType]],
                               withCustomAttributes: Option[Seq[String]],
                               timeoutOffset: Int)
                              (doWork: Message[T] => Unit)
                              (implicit queue: Queue, system: ActorSystem, reader: Reader[T])
    extends QueueListener[T](maxMessages, wait, timeout, withAttributes, withCustomAttributes, timeoutOffset)(queue, system, reader) {

  override val log = LoggerFactory.getLogger(this.getClass)

  log.info(s"Setting input buffer size to $inputBufferSize")

  /**
    * Executes worker function
    * Wraps execution of the worker function in Try, in case implementation did not handle exception,
    * logs the message and re-throws the exception, recovery will be handled by the supervisionStrategy (decider)
    */
  protected def sink: Sink[Message[T], Future[Unit]] = Sink.foreach[Message[T]] { msg =>
    Try(doWork(msg)) match {
      case Failure(e) => {
        log.error( s"""Unhandled queue worker failure, queue: '${queue.url}',
                        error: ${e.getMessage}.Stack Trace: ${e.printStackTrace}""".stripMargin)
        throw e
      }
      case _ =>
    }
  }

  protected def graph[U](source: Source[Message[T], ActorRef],
                         sink: Sink[Message[T], U]): RunnableGraph[Unit] =

    RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      log.debug(s"Creating balanced RunnableGraph with $workerCount workers...")

      val bcast = builder.add(Balance[Message[T]](workerCount))
      source ~> bcast.in
      for(i <- 0 until workerCount) bcast.out(i) ~> sink
      ClosedShape

    }.named("QueueWorkerBalanceFlowGraph")
      .withAttributes(ActorAttributes.supervisionStrategy(decider)))


  /**
    * Run only once
    */
  override lazy val run = graph(source, sink) run
}
