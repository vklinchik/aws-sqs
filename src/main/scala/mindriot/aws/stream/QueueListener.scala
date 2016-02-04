package mindriot.aws.stream

import akka.actor._
import akka.stream.scaladsl._
import akka.stream._
import org.slf4j.LoggerFactory

import mindriot.aws.sqs.{Reader, AttributeType, Queue, Message}

abstract class QueueListener[T](maxMessages: Int,
                                wait: Option[Int] = None,
                                timeout: Option[Int] = None,
                                withAttributes: Option[Seq[AttributeType]] = None,
                                withCustomAttributes: Option[Seq[String]] = None,
                                timeoutOffset: Int = 0)
                               (implicit queue: Queue, system: ActorSystem, reader: Reader[T]) {


  val log = LoggerFactory.getLogger(this.getClass)


  /**
    * Override, if the custom handling is required.
    * Current definition restarts the worker in case of any failure.
    */
  val decider: Supervision.Decider = {
    case _: ActorInitializationException => Supervision.stop
    case _: ActorKilledException => Supervision.stop
    case _: Exception => Supervision.restart
    case _: Throwable => Supervision.stop
  }

  def inputBufferSize = 2

  val matSettings = ActorMaterializerSettings(system).withSupervisionStrategy(decider)
    .withInputBuffer(initialSize = inputBufferSize, maxSize = inputBufferSize)
  implicit val materializer = ActorMaterializer(matSettings).withNamePrefix("QueueListenerMat")


  /**
    * Create publisher and attach to soource
    */
  protected val source: Source[Message[T], ActorRef] =
    Source.actorPublisher[Message[T]](Props(classOf[QueueListenerActor[T]], maxMessages, wait, timeout, withAttributes, withCustomAttributes, timeoutOffset, queue, system, reader))
                              .named("QueueListeningActor")
                              .withAttributes(ActorAttributes.supervisionStrategy(decider))


  /**
    * Run the flow
    */
  def run


}

