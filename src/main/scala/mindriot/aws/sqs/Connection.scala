package mindriot.aws.sqs

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.{Regions, Region}
import com.amazonaws.services.sqs.{AmazonSQSAsyncClient, AmazonSQSAsync}


import scala.concurrent.{Future, Promise}


trait Connection {

  val client: AmazonSQSAsync

  def close: Unit

}

object Connection {

  import com.typesafe.config.{ConfigFactory}

  private[this] val config = ConfigFactory.load()
  private[this] val accessKey = config.getString("aws.accessKey")
  private[this] val secretKey = config.getString("aws.secretKey")
  private[this] val endPoint = config.getString("aws.endpoint")

  private[this] val awsCredentials: AWSCredentials = new BasicAWSCredentials(accessKey, secretKey)

  def apply(): Connection = {
    val sqs: AmazonSQSAsync = new AmazonSQSAsyncClient(awsCredentials)
    sqs.setEndpoint(endPoint)
    val usWest = Region.getRegion(Regions.US_WEST_2)
    sqs.setRegion(usWest)

    new Connection {
      override val client: AmazonSQSAsync = sqs
      override def close = client shutdown
    }
  }
}
