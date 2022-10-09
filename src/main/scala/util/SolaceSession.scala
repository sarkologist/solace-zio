package util

import java.time.Duration

import com.solacesystems.jcsmp._
import org.slf4j.{Logger, LoggerFactory}
import zio.{Queue, Task, URIO, ZIO}

object SolaceSession {
  sealed trait QueryException[+A] extends Exception {
    def display: String
  }

  case class TimedOutException(duration: zio.duration.Duration)
      extends QueryException[Nothing] {
    override def display: String = s"TimedOut($duration)"
  }

  case class NetworkException(networkException: JCSMPException)
      extends QueryException[Nothing] {
    override def display: String = s"NetworkException($networkException)"
  }

  sealed trait UnrecoverableException[+A] extends QueryException[A]
  case class OtherException(throwable: Throwable)
      extends UnrecoverableException[Nothing] {
    override def display: String = s"OtherException($throwable)"
  }
  case class BadResponse[+A](response: A) extends UnrecoverableException[A] {
    override def display: String = s"BadResponse($response)"
  }

  def withProperties[A](jcsmpProperties: JCSMPProperties)(
      compute: JCSMPSession => A): A = {
    var session: JCSMPSession = null
    var result: A             = null.asInstanceOf[A]
    try {
      session = JCSMPFactory
        .onlyInstance()
        .createSession(jcsmpProperties)
      session.connect()
      result = compute(session)
    } catch {
      case e: JCSMPException =>
        LOG.error("error while opening solace session {}", e)
    } finally {
      Option(session).foreach(_.closeSession())
    }
    result
  }

  def openSolaceListener[R, E](jcsmpProperties: JCSMPProperties)
    : ZIO[R, QueryException[E], SolaceListener[R]] =
    ZIO
      .effect {

        val session = JCSMPFactory
          .onlyInstance()
          .createSession(jcsmpProperties)

        val listener = new SolaceListener[R](session)

        session.getMessageProducer(emptyMessageProducer)

        val solaceConsumer: XMLMessageConsumer =
          session.getMessageConsumer(listener)
        solaceConsumer.start()
        session.connect()

        Thread.sleep(subscriptionPropagationWaitTime.toMillis)

        listener
      }
      .refineToOrDie[JCSMPException]
      .mapError(NetworkException)

  val emptyMessageProducer: JCSMPStreamingPublishEventHandler =
    new JCSMPStreamingPublishEventHandler() {
      @Override def responseReceived(messageID: String): Unit = {}
      @Override def handleError(messageID: String,
                                e: JCSMPException,
                                timestamp: Long): Unit = {}
    }

  val emptyMessageListener: XMLMessageListener = new XMLMessageListener {
    override def onReceive(bytesXMLMessage: BytesXMLMessage): Unit = {}
    override def onException(e: JCSMPException): Unit              = {}
  }

  sealed trait MessageCorrelation
  case class ByTopicAndCorrelationId(destinationTopic: String,
                                     correlationId: String)
      extends MessageCorrelation
  case class ByTopicOnly(destinationTopic: String) extends MessageCorrelation

  class SolaceListener[R](val session: JCSMPSession)
      extends XMLMessageListener {
    type Callback = ZIO[R, QueryException[Nothing], Array[Byte]] => Unit

    var registry = Map.empty[MessageCorrelation, Callback]

    def register(correlation: MessageCorrelation, cb: Callback): Unit =
      this.synchronized {
        registry += (correlation -> cb)
      }

    def unregister(correlation: MessageCorrelation): Unit =
      this.synchronized {
        registry -= correlation
      }

    def close: URIO[R, Unit] =
      ZIO.succeed(session.closeSession())

    def defaultReplyTo: String =
      session
        .getProperty(JCSMPProperties.P2PINBOX_IN_USE)
        .asInstanceOf[Destination]
        .getName

    def request(topic: Topic, requestData: Array[Byte])
      : ZIO[R, QueryException[Nothing], Array[Byte]] =
      ZIO.effectAsync[R, QueryException[Nothing], Array[Byte]] { cb =>
        val request =
          JCSMPFactory.onlyInstance().createMessage(classOf[BytesMessage])
        request.setData(requestData)
        request.setCorrelationId(java.util.UUID.randomUUID.toString)

        val correlation =
          ByTopicAndCorrelationId(defaultReplyTo, request.getCorrelationId)
        register(
          correlation,
          value =>
            cb(Task.effectTotal(
              LOG.info("received single-response for topic: {}", topic)) *> Task
              .effectTotal(unregister(correlation)) *> value)
        )

        session.createRequestor().request(request, 0, topic)
      }

    def requestMultiResponse(topic: Topic, requestData: Array[Byte])
      : ZIO[R, QueryException[Nothing], FutureMessages[R, Array[Byte]]] = {
      def send(rts: zio.Runtime[R], queue: Queue[Array[Byte]])
        : ZIO[R, QueryException[Nothing], FutureMessages[R, Array[Byte]]] =
        ZIO
          .effect {
            val correlation =
              ByTopicAndCorrelationId(defaultReplyTo,
                                      java.util.UUID.randomUUID.toString)

            val request =
              JCSMPFactory.onlyInstance.createMessage(classOf[BytesMessage])
            request.setData(requestData)
            request.setCorrelationId(correlation.correlationId)
            register(
              correlation,
              result =>
                rts.unsafeRunSync(
                  Task.effectTotal(
                    LOG.info("received multi-response for topic: {}", topic)) *>
                    result.flatMap(queue.offer)))
            session.createRequestor.request(request, 0, topic)
            FutureMessages[R, Array[Byte]](
              queue,
              Task.effectTotal(unregister(correlation)))
          }
          .refineToOrDie[JCSMPException]
          .mapError(NetworkException)

      for {
        queue          <- Queue.unbounded[Array[Byte]]
        rts            <- ZIO.runtime[R]
        futureMessages <- send(rts, queue)
      } yield futureMessages
    }

    def futureMessagesFromTopic(topic: Topic)
      : ZIO[R, QueryException[Nothing], FutureMessages[R, Array[Byte]]] =
      for {
        rts   <- ZIO.runtime[R]
        queue <- Queue.unbounded[Array[Byte]]
      } yield {
        session.addSubscription(
          null.asInstanceOf[Endpoint],
          topic,
          JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS | JCSMPSession.WAIT_FOR_CONFIRM)
        val correlation = ByTopicOnly(topic.getName)
        register(
          correlation,
          result =>
            rts.unsafeRunSync(
              Task.effectTotal(
                LOG.debug("received future message for topic: {}", topic)) *>
                result
                  .flatMap(queue.offer)
          )
        )
        FutureMessages(queue, Task.effectTotal {
          unregister(correlation)
          session.removeSubscription(topic)
        })
      }

    override def onReceive(message: BytesXMLMessage): Unit = {
      registry
        .get(ByTopicAndCorrelationId(message.getDestination.getName,
                                     message.getCorrelationId))
        .orElse(
          registry
            .get(ByTopicOnly(message.getDestination.getName))
        )
        .fold(
          LOG.warn("could not look up callback {} {} in registry",
                   List(message.getDestination.getName,
                        message.getCorrelationId): _*)) { cb =>
          message match {
            case msg: BytesMessage =>
              cb(ZIO.succeed(msg.getData))
            case _ =>
              cb(ZIO.fail(
                OtherException(new RuntimeException("expected BytesMessage"))))
          }
        }
    }

    override def onException(e: JCSMPException): Unit =
      registry.values.foreach { cb =>
        cb(
          ZIO.fail(OtherException(
            new RuntimeException(s"exception in XMLMessageConsumer: $e"))))
      }
    registry = Map.empty
  }

  case class FutureMessages[R, A](queue: Queue[A], finalizeZio: URIO[R, Unit])

  val LOG: Logger                               = LoggerFactory.getLogger(SolaceSession.getClass)
  val subscriptionPropagationWaitTime: Duration = Duration.ofSeconds(10)
}
