package mindriot.aws.sqs


import scala.concurrent.{ExecutionContext, Promise, Future}

trait Message[T] {


  def id: String
  def body: T

  def attributes: MessageAttributes[String]
  def messageAttributes: CustomMessageAttributes

  /**
    * Acknowledge message receipt, deletes it from the queue
    * @return
    */
  def ack: Future[Unit]

  /**
    * Don't acknowledge message receipt, usually called in case of failure to
    * process the message and sets visibility property to true
    * @return
    */
  def nack: Future[Unit]


  /**
    * Limit on how long message can be held in memory. Milliseconds
    */
  //def timeout: Long

  /**
    * Check message expiration
    * @return
    */
  //def expired: Boolean
}





private[sqs] case class MessageImpl[T](override val id: String,
                                       override val body: T,
                                       override val attributes: MessageAttributes[String],
                                       override val messageAttributes: CustomMessageAttributes,
                                       queue: Queue,
                                       receiptHandle: String)
                                      (implicit ec: ExecutionContext) extends Message[T] {


  import com.amazonaws.handlers.AsyncHandler
  import com.amazonaws.services.sqs.model.DeleteMessageRequest

  override def ack: Future[Unit] = {
    val request = new DeleteMessageRequest(queue.url, receiptHandle)
    val p = Promise[Unit]()

    val handler = new AsyncHandler[DeleteMessageRequest, Void] {
      override def onSuccess(req: DeleteMessageRequest, res: Void) = p success(Unit)
      override def onError(err: Exception) = p failure(err)
    }
    queue.connection.client.deleteMessageAsync(request, handler)
    p future
  }

  override def nack: Future[Unit] = Future(Unit)
}