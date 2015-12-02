package mindriot.aws.sqs

import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.model.{Message => SQSMessage, _}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Promise, Future}


trait Queue {

  def url: String
  def connection: Connection

  /**
    * List Queue Attributes
    * @param types
    * @return
    */
  def attributes(types: AttributeType *): Future[QueueAttributes[String]]

  /**
    * Send message to a queue
    * @param message
    * @param customAttributes
    * @param delay
    * @param writer
    * @tparam T
    * @return
    */
  def send[T](message: T,
              customAttributes: CustomMessageAttributes = Map.empty[String, MessageAttributeValue],
              delay: Option[Int] = None)
             (implicit writer: Writer[T]): Future[Unit]


  /**
    * Fetch specified number of messages from the queue
    * @param maxMessages Max number of messages to fetch
    * @param wait Duration in seconds for which the call will wait for a message to arrive in the queue if non exists
    * @param timeout Visibility timeout in seconds - duration of received message hidden from subsequent retrieve requests.
    * @param withAttributes List of attributes to fetch
    * @param withCustomAttributes List of custom message attributes to fetch
    * @param reader
    * @param ec
    * @tparam T
    * @return
    */
  def receive[T](maxMessages: Int = 1, wait: Option[Int] = None, timeout: Option[Int] = None,
                 withAttributes: Option[Seq[AttributeType]] = None,
                 withCustomAttributes: Option[Seq[String]] = None)
                (implicit reader: Reader[T], ec: ExecutionContext): Future[List[Message[T]]]


  /**
    * Fetch single message from the queue
    * @param wait Duration in seconds for which the call will wait for a message to arrive in the queue if non exists
    * @param timeout Visibility timeout in seconds - duration of received message hidden from subsequent retrieve requests.
    * @param withAttributes List of attributes to fetch
    * @param withCustomAttributes List of custom message attributes to fetch
    * @param reader Message reader
    * @param ec
    * @tparam T
    * @return
    */
  def receiveOne[T](wait: Option[Int] = None, timeout: Option[Int],
                    withAttributes: Option[Seq[AttributeType]] = None,
                    withCustomAttributes: Option[Seq[String]] = None)
                   (implicit reader: Reader[T], ec: ExecutionContext): Future[Option[Message[T]]]




  /*


  def addPermission(): Future[Unit]
  def removePermission(): Future[Unit]
  def changeMessageVisibility(msg: Message[T]): Future[Unit]
  def changeMessageVisibility(msgs: List[Message]): Future[Unit]
  def purge(): Future[Unit]
  */

}

private[sqs] case class QueueImpl(override val url: String, override val connection: Connection) extends Queue {

  self =>

  private[this] val client = connection.client

  def attributes(types: AttributeType *): Future[QueueAttributes[String]] = {

    val attributes = seqAsJavaList(
      types match {
        case Nil => Seq(QueueAttributeType.All.name)
        case _ => types.map(_.name)
      }
    )
    val request = new GetQueueAttributesRequest(url, attributes)
    val p: Promise[QueueAttributes[String]] = Promise()

    val handler = new AsyncHandler[GetQueueAttributesRequest, GetQueueAttributesResult] {
      override def onSuccess(req: GetQueueAttributesRequest, res: GetQueueAttributesResult) =
        p.success(
          res
            .getAttributes
            .toMap
            .map(kv => (stringToQueueAttributeType(kv._1), kv._2))
        )

      override def onError(err: Exception) = p.failure(err)
    }

    client.getQueueAttributesAsync(request, handler)
    p future
  }


  def send[T](message: T,
              customAttributes: CustomMessageAttributes = Map.empty[String, MessageAttributeValue],
              delay: Option[Int] = None)
             (implicit writer: Writer[T]): Future[Unit] = {

    val request = new SendMessageRequest(url, writer.write(message))
    delay.foreach(request.setDelaySeconds(_))
    customAttributes.foreach{
      case (k, v) => request.addMessageAttributesEntry(k, v)
    }

    val p = Promise[Unit]()

    val hanlder = new AsyncHandler[SendMessageRequest, SendMessageResult] {
      override def onSuccess(req: SendMessageRequest, res: SendMessageResult) = p.success(Unit)
      override def onError(err: Exception) = p.failure(err)
    }

    client.sendMessageAsync(request, hanlder)
    p future
  }


  def receive[T](maxMessages: Int = 1, wait: Option[Int] = None, timeout: Option[Int] = None,
                 withAttributes: Option[Seq[AttributeType]] = None,
                 withCustomAttributes: Option[Seq[String]] = None)
                (implicit reader: Reader[T], ec: ExecutionContext): Future[List[Message[T]]] = {

    val request = new ReceiveMessageRequest(url).withMaxNumberOfMessages(maxMessages)
    wait.foreach(request.withWaitTimeSeconds(_))
    timeout.foreach(request.withVisibilityTimeout(_))
    withAttributes.foreach(seq => request.setAttributeNames(seq.map(_.name)))
    withCustomAttributes.foreach(request.setMessageAttributeNames(_))


    val p: Promise[List[Message[T]]] = Promise()

    val handler = new AsyncHandler[ReceiveMessageRequest, ReceiveMessageResult] {

      override def onSuccess(req: ReceiveMessageRequest, res: ReceiveMessageResult) = {
        val list = res.getMessages.asScala.foldRight(List[Message[T]]()){ (msg, l) =>
          MessageImpl(msg.getMessageId,
                      reader.read(msg.getBody),
                      msg.getAttributes.toMap.map( kv => (stringToMessageAttributeType(kv._1), kv._2)),
                      msg.getMessageAttributes.asScala.toMap, //CustomMessageAttributes
                      self,
                      msg.getReceiptHandle) :: l
        }
        p.success(list)
      }

      override def onError(err: Exception) = p.failure(err)
    }

    client.receiveMessageAsync(request, handler)
    p future
  }


  def receiveOne[T](wait: Option[Int] = None, timeout: Option[Int],
                    withAttributes: Option[Seq[AttributeType]] = None,
                    withCustomAttributes: Option[Seq[String]] = None)
                   (implicit reader: Reader[T], ec: ExecutionContext): Future[Option[Message[T]]] =
    receive(1, wait, timeout, withAttributes, withCustomAttributes).map(_.headOption)

}



object Queue {

  /**
    * Create queue with a specified name
    * @param queueName Name of the queue
    * @param cn
    * @return
    */
  def create(queueName: String)(implicit cn: Connection, ec: ExecutionContext): Future[Queue] = {
    val request = new CreateQueueRequest().withQueueName(queueName)
    val p = Promise[Queue]()

    val handler = new AsyncHandler[CreateQueueRequest, CreateQueueResult] {
      override def onSuccess(req: CreateQueueRequest, res: CreateQueueResult) = apply(res getQueueUrl).map(_.head)
      override def onError(err: Exception) = p.failure(err)
    }

    cn.client.createQueueAsync(request, handler)
    p future
  }

  /**
    * Delete queue specified by url parameter
    * @param url URL of the queue
    * @param cn implicit connection
    * @return Unit
    */
  def delete(url: String)(implicit cn: Connection): Future[Unit] = {
    val request = new DeleteQueueRequest().withQueueUrl(url)
    val p = Promise[Unit]()

    cn.client.deleteQueueAsync(request, new AsyncHandler[DeleteQueueRequest, Void] {
      override def onSuccess(request: DeleteQueueRequest, result: Void) = p.success(Unit)
      override def onError(err: Exception) = p.failure(err)
    })

    p future
  }

  /**
    * Lists all the queues with specified name prefix.
    * Leave prefix empty to list all queues, or specified exact name to locate queue based on name.
    * @param namePrefix
    * @param cn
    * @return
    */
  def list(namePrefix: String = "")(implicit cn: Connection): Future[List[Queue]] = {
    val request = new ListQueuesRequest().withQueueNamePrefix(namePrefix)
    val p = Promise[List[Queue]]()

    cn.client.listQueuesAsync(request, new AsyncHandler[ListQueuesRequest, ListQueuesResult] {
      override def onSuccess(req: ListQueuesRequest, res: ListQueuesResult) =
        p.success(res.getQueueUrls.toList.map(QueueImpl(_, cn)))

      override def onError(err: Exception) = p.failure(err)
    })

    p future
  }

  /**
    * Finds queue object based on the url
    * @param url Queue url
    * @param cn
    * @param ec
    * @return
    */
  def apply(url: String)(implicit cn: Connection, ec: ExecutionContext): Future[Option[Queue]] = {
    val name = url.split("/").last
    list(name).map(_.filter(_.url == url).headOption)
  }


}
