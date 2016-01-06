package mindriot.aws.sqs


trait Format[T] {
  type Value = String
}

trait Writer[T] extends Format[T] {
  def write(obj: T): Value
}

trait Reader[T]  extends Format[T] {
  def read(value: Value): T
}

object StringFormat {

  object StringWriter extends Writer[String] {
    override def write(obj: String) = obj
  }

  object StringReader extends Reader[String] {
    override def read(value: Value): String = value
  }

  implicit val reader = StringReader
  implicit val writer = StringWriter
}



object Base64Format {
  import java.util.Base64.{getEncoder, getDecoder}

  object Base64Reader extends Reader[Array[Byte]] {
    override def read(value: Value): Array[Byte] = getEncoder.encode(value.getBytes)
  }

  object Base64Writer extends Writer[Array[Byte]] {
    override def write(obj: Array[Byte]) = new String(getDecoder.decode(obj))
  }

  implicit val reader = Base64Reader
  implicit val writer = Base64Writer

}