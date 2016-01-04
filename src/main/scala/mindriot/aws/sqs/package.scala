package mindriot.aws


package object sqs {

  type QueueAttributes[T] = Map[AttributeType, T]

  sealed abstract class AttributeType(val name: String) {
    override def toString = name
  }

  /**
    * Queue Attribute types
    */
  object QueueAttributeType {

    val All = new AttributeType("All") {}

    val ApproximateNumberOfMessagesDelayed = new AttributeType("ApproximateNumberOfMessagesDelayed"){}
    val VisibilityTimeout = new AttributeType("VisibilityTimeout") {}
    val ApproximateNumberOfMessagesNotVisible = new AttributeType("ApproximateNumberOfMessagesNotVisible") {}
    val LastModifiedTimestamp = new AttributeType("LastModifiedTimestamp") {}
    val Policy = new AttributeType("Policy") {}
    val QueueArn = new AttributeType("QueueArn") {}
    val CreatedTimestamp = new AttributeType("CreatedTimestamp") {}
    val MessageRetentionPeriod = new AttributeType("MessageRetentionPeriod") {}
    val ApproximateNumberOfMessages = new AttributeType("ApproximateNumberOfMessages") {}
    val ReceiveMessageWaitTimeSeconds = new AttributeType("ReceiveMessageWaitTimeSeconds") {}
    val DelaySeconds = new AttributeType("DelaySeconds") {}
    val MaximumMessageSize = new AttributeType("MaximumMessageSize") {}

    val values = List(ApproximateNumberOfMessagesDelayed,
      VisibilityTimeout,
      ApproximateNumberOfMessagesNotVisible,
      LastModifiedTimestamp,
      Policy,
      QueueArn,
      CreatedTimestamp,
      MessageRetentionPeriod,
      ApproximateNumberOfMessages,
      ReceiveMessageWaitTimeSeconds,
      DelaySeconds,
      MaximumMessageSize)

    def apply(name: String): Option[AttributeType] = values.find(_.name == name)
    def unapply(at: AttributeType): Option[String] = Some(at.name)
  }

  implicit def stringToQueueAttributeType(at: String): AttributeType =
    QueueAttributeType.apply(at).getOrElse(throw new IllegalArgumentException("Can't resolve queue attribute type."))


  type MessageAttributes[T] = Map[AttributeType, T]

  object MessageAttributeType {

    val SenderId = new AttributeType("SenderId") {}
    val SentTimestamp = new AttributeType("SentTimestamp") {}
    val ApproximateReceiveCount = new AttributeType("ApproximateReceiveCount") {}
    val ApproximateFirstReceiveTimestamp = new AttributeType("ApproximateFirstReceiveTimestamp") {}

    val values = List(SenderId, SentTimestamp, ApproximateReceiveCount, ApproximateFirstReceiveTimestamp)

    def apply(name: String): Option[AttributeType] = values.find(_.name == name)
    def unapply(at: AttributeType): Option[String] = Some(at.name)
  }

  implicit def stringToMessageAttributeType(at: String): AttributeType =
    MessageAttributeType.apply(at).getOrElse(throw new IllegalArgumentException("Can't resolve message attribute type."))


  import com.amazonaws.services.sqs.model.MessageAttributeValue

  type CustomMessageAttributes = Map[String, MessageAttributeValue]


  /**
    * Queue Permissions
    * @param name
    */
  sealed abstract class Permission(val name: String) {
    override def toString = name
  }

  object Permission {
    val All = new Permission("*"){}

    val ReceiveMessage = new Permission("ReceiveMessage") {}
    val SendMessage = new Permission("SendMessage"){}
    val DeleteMessage = new Permission("DeleteMesssage"){}
    val ChangeMessageVisibility = new Permission("ChangeMessageVisibility"){}
    val GetQueueAttributes = new Permission("GetQueueAttributes"){}
    val GetQueueUrl = new Permission("GetQueueUrl") {}
    val PurgeQueue = new Permission("PurgeQueue") {}

    val values = List(All, ReceiveMessage, SendMessage, DeleteMessage, ChangeMessageVisibility,
      GetQueueAttributes, GetQueueUrl, PurgeQueue)

    def apply(name: String): Option[Permission] = values.find(_.name == name)
    def unapply(p: Permission): Option[String] = Some(p.name)
  }
}
