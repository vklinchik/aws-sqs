import org.specs2.mutable.Specification
import mindriot.aws.sqs.{StringFormat, Base64Format}

class FormatSpec extends Specification {

  val stringValue = "Be thankful we're not getting all the government we're paying for."

  "Format" should {
    "read and write strings" in {
      val s = StringFormat.reader.read(stringValue)
      StringFormat.writer.write(s) must beEqualTo(stringValue)
    }

    "read and write to array" in {
      val b = Base64Format.reader.read(stringValue)
      Base64Format.writer.write(b) must beEqualTo(stringValue)
    }

  }
}
