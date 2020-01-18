package com.example.kafkaToRabbitmq

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerMessage, ConsumerSettings, Subscriptions}
import akka.stream.alpakka.amqp.scaladsl.AmqpRpcFlow
import akka.stream.alpakka.amqp.{AmqpLocalConnectionProvider, AmqpWriteSettings, QueueDeclaration}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RestartFlow, Sink, Source, Zip}
import akka.stream.{ActorMaterializer, FlowShape}
import akka.util.ByteString
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

// at least once implementation of a alpakka kafka akka stream job that writes to MQ
class Main extends App {
  implicit val system: ActorSystem = ActorSystem("KafkaToRabbitMQ")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  implicit val ec: ExecutionContext = system.dispatcher

  val consumerSettings =
    ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId("group1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")


  val source: Source[ConsumerMessage.CommittableMessage[String, String], Consumer.Control] =
    Consumer
      .committableSource(
        consumerSettings,
        Subscriptions.topics("testtopic")
      )

  val committerSettings = CommitterSettings(system)

  val connectionProvider = AmqpLocalConnectionProvider
  val queueName = "amqp-conn-it-spec-simple-queue-" + System.currentTimeMillis()
  val queueDeclaration = QueueDeclaration(queueName)

  val amqpRpcFlow: Flow[ByteString, ByteString, Future[String]] = AmqpRpcFlow.simple(
    AmqpWriteSettings(connectionProvider).withRoutingKey(queueName).withDeclaration(queueDeclaration)
  )

  val committerSink: Sink[ConsumerMessage.Committable, Future[Done]] = Committer.sink(committerSettings)

  // since alpakka rabbitMQ doesnt have a passthrough flow, the best way to ensure that
  // the records can be consumed at least once is to create a custom graph to split the flow into 2
  // one for passing through the kafka commitable message, the other for MQ flow.
  // in the end, zip the 2 flows together and commit the offset using a commiter sink
  val pairUpCommittableWithMQ: Flow[ConsumerMessage.CommittableMessage[String, String], (ConsumerMessage.Committable, ByteString), NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      // prepare graph elements
      val broadcast = b.add(Broadcast[ConsumerMessage.CommittableMessage[String, String]](2))
      val zip = b.add(Zip[ConsumerMessage.Committable, ByteString]())

      // connect the graph
      broadcast.out(0).map(_.committableOffset) ~> zip.in0
      broadcast.out(1).map{ cr =>
        ByteString(cr.record.value())
      } ~> amqpRpcFlow ~> zip.in1

      // expose ports
      FlowShape(broadcast.in, zip.out)
    })

  //if pairUpCommittableWithMQ fails, retry
  val restartFlow: Flow[ConsumerMessage.CommittableMessage[String, String], (ConsumerMessage.Committable, ByteString), NotUsed] = RestartFlow.withBackoff(
    minBackoff = 3.seconds,
    maxBackoff = 30.seconds,
    randomFactor = 0.2, // adds 20% "noise" to vary the intervals slightly
    maxRestarts = 20 // limits the amount of restarts to 20
  ) { () =>
    pairUpCommittableWithMQ
  }

  source
    .via(restartFlow)
    .map(_._1)
    .runWith(committerSink)
}
