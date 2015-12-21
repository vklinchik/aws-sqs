package mindriot.aws.sqs

import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, AWSCredentialsProvider, AWSCredentials}
import com.amazonaws.regions.{Regions, Region}
import com.amazonaws.services.sqs.{AmazonSQSAsyncClient, AmazonSQSAsync}



trait Connection {

  val client: AmazonSQSAsync

  def close: Unit

}

object Connection {

  def apply(provider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain(),
            regions: Regions = Regions.DEFAULT_REGION): Connection =
    apply(provider getCredentials, regions)


  def apply(credentials: AWSCredentials,
            regions: Regions): Connection = {

    val sqs: AmazonSQSAsync = new AmazonSQSAsyncClient(credentials)
    sqs.setRegion(Region.getRegion(regions))

    new Connection {
      override val client: AmazonSQSAsync = sqs
      override def close = client shutdown
    }
  }
}
