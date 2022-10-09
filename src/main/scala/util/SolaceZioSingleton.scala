package util

import java.util.concurrent.TimeUnit

import com.solacesystems.jcsmp.JCSMPProperties
import org.slf4j.{Logger, LoggerFactory}
import util.SolaceSession.{
  NetworkException,
  QueryException,
  SolaceListener,
  TimedOutException
}
import zio.{DefaultRuntime, Schedule, Task, ZEnv, ZIO}

/*
use when you need to make solace requests in parallel across beam shards,
to ensure we don't exceed solace connection limits

there will be one instance of this singleton per worker (assuming one jvm per worker)
 */
object SolaceZioSingleton {
  val zioRuntime: DefaultRuntime     = new DefaultRuntime {}
  var listener: SolaceListener[ZEnv] = _

  def getOrInitialiseListener[E](
      jcsmpProperties: JCSMPProperties): SolaceListener[ZEnv] =
    synchronized[SolaceListener[ZEnv]] {
      if (listener == null) {
        val retrySchedule = Schedule
          .exponential(zio.duration.Duration(5, TimeUnit.SECONDS)) || Schedule
          .fixed(zio.duration.Duration(5, TimeUnit.MINUTES))
          .whileInput((e: QueryException[E]) =>
            e.isInstanceOf[NetworkException] || e
              .isInstanceOf[TimedOutException])
          .jittered()

        listener = zioRuntime.unsafeRun(
          SolaceSession
            .openSolaceListener[ZEnv, E](jcsmpProperties)
            .tap(_ =>
              ZIO.effectTotal(LOG.info("{}: establish solace session: success",
                                       solaceZioSingletonLogPrefix)))
            .tapError(
              e =>
                Task.effectTotal(
                  LOG
                    .warn("{}: establish session: retry: {}",
                          List(solaceZioSingletonLogPrefix, e.display): _*)))
            .retry(retrySchedule)
        )
      }

      listener
    }

  val solaceZioSingletonLogPrefix = "SolaceZioSingleton"
  val LOG: Logger                 = LoggerFactory.getLogger(SolaceZioSingleton.getClass)
}
