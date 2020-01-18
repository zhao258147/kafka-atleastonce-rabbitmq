name := "kafka-to-rabbitmq"

version := "0.1"

scalaVersion := "2.13.1"

libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-amqp" % "1.1.1"
libraryDependencies += "com.typesafe.akka" %% "akka-stream-kafka" % "1.0.5"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.23"

