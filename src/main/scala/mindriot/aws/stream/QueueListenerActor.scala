package mindriot.aws.stream

import akka.actor.{ActorSystem, ActorLogging}
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import com.amazonaws.{AmazonServiceException, AmazonClientException}
import org.joda.time.{Instant, Interval}
import scala.util.{Success, Failure}
import scala.collection.mutable.{Queue => MQueue}
import scala.concurrent.duration._

import mindriot.aws.sqs._


private[stream] class QueueListenerActor[T](maxMessages: Int,
                                         wait: Option[Int] = None,
                                         timeout: Option[Int] = None,
                                         withAttributes: Option[Seq[AttributeType]] = None,
                                         withCustomAttributes: Option[Seq[String]] = None,
                                         timeoutOffset: Int = 0)
                                        (implicit queue: Queue, system: ActorSystem, reader: Reader[T])
  extends ActorPublisher[Message[T]] with ActorLogging {

  val msgQueue = new MQueue[Message[_<:T]]

  implicit val ec = system.dispatcher

  // queue visibility timeout
  var visibilityTimeout: Int = -1
  var processedMsgCount = 0
  val startInstant = new Instant

  case object RefreshVisibilityTimeout

  /**
    * Create a scheduler to regulary refresh queue visibility timeout in case queue is re-configured
    */
  context.system.scheduler.schedule(30 minutes, 30 minutes, self, RefreshVisibilityTimeout)(context.system.dispatcher)



  override def preStart = {
    super.preStart
    log.debug("preStart: QueueListenerActor")
    refreshVisibiltyTimeout
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.debug("preRestart: QueueListenerActor")
    super.preRestart(reason, message)
  }

  override def postStop(): Unit = {
    val uptime = calculateDuration(startInstant)
    val msg = s"""Shutting down actor.
                  |Total message available for processing = ${msgQueue.length}.
                  |Total processed message count(lifecycle) = ${processedMsgCount}
                  |Approximate uptime: ${uptime._1} ${uptime._2}""".stripMargin
    log.info(msg)
    super.postStop
  }

  override def postRestart(reason: Throwable): Unit = {
    log.debug("postRestart")
    super.postRestart(reason)
  }


  def receive = {
    case Request(demand) =>
      log.debug("Listener(publisher) received request from subscriber")
      send
    case RefreshVisibilityTimeout =>
      log.info("Refreshing visiblity timeout")
      refreshVisibiltyTimeout
    case e: Exception =>
      log.error(s"Forced exception in QueueListener, crashing: ${e.getMessage}")
      throw e
    case Cancel =>
      log.info(s"Listener(publisher) recieved Cancel message from ${sender.path.name} - stopping....")
      context stop self
  }


  def send: Unit = {
    log.debug(s"Total Demand = $totalDemand")
    while(isActive && totalDemand > 0) {
      getMessages
      if( !msgQueue.isEmpty ) msgQueue.dequeue match {
        case msg => {
          //TODO: message visibility timeout expiration
          //if(!msg.expired) {
            //TODO: Refactor asInstanceOf
            onNext(msg.asInstanceOf[Message[T]])
            processedMsgCount += 1
            log.debug(s"Message with id: ${msg.id} sent to worker")
          //}
        }
      }
    }
  }


  /**
    * Load message into internal queue only if the queue is empty and visibility timeout is set
    */
  def getMessages = {

    def loadMessages = {
      val cnt = msgQueue.length
      val lst = queue.receiveSync(maxMessages, wait, timeout, withAttributes, withCustomAttributes)
      msgQueue ++= lst
        log.debug(
            s"""Received ${msgQueue.length - cnt} message(s).
                |Total message available for processing = ${msgQueue.length}.
                |Total processed message count(lifecycle) = ${processedMsgCount}""".stripMargin)
    }

    if( msgQueue.length == 0 && visibilityTimeout >= 0 ) loadMessages

  }

  /**
    * Calculates approximate duration from a given instant
    *
    * @param start
    * @return
    */
  def calculateDuration(start: Instant) : (Long, String) = {
    val interval = new Interval(start, new Instant)
    val duration = interval.toDuration

    if( duration.getStandardSeconds / 60 > 0 )
      if( duration.getStandardMinutes / 60 > 0 )
        (duration.getStandardHours, "hour(s)")
      else (duration.getStandardMinutes , "minute(s)")
    else (duration.getStandardSeconds, "second(s)")
  }


  def refreshVisibiltyTimeout = {

    queue.attributes(QueueAttributeType.VisibilityTimeout).onComplete {
      case Success(attrs) => {
        visibilityTimeout = attrs(QueueAttributeType.VisibilityTimeout).toInt - timeoutOffset
        log.info(s"VisibilityTimeout with timeoutOffset is set to ${visibilityTimeout}s for Queue '${queue.url}'")
      }
      case Failure(t) => {
        t match {
          case se: AmazonServiceException => handleServiceException(se)
          case ce: AmazonClientException =>  handleClientException(ce)
        }
      }
    }

  }


  /**
    * Request made it to amazon, but was rejected
    *
    * @param e
    */
  def handleServiceException(e: AmazonServiceException): Unit = {
    val err =
      s"""AWS service exception: ${e.getMessage}.
          |HTTP error code: ${e.getStatusCode},
          |AWS error code: ${e.getErrorCode},
          |Error type: ${e.getErrorType},
          |Request Id: ${e.getRequestId},
          |Service name: ${e.getServiceName}
       """.stripMargin

    log.error(err)
  }

  /**
    * Client encountered problem while trying to communicate with SQS (network?)
    *
    * @param e
    */
  def handleClientException(e: AmazonClientException) = {
    val err = s"""AWS client exception: ${e.getMessage}"""
    log.error(err)
  }
}