package io.jobial.sprint.util

import cats.effect.Concurrent
import cats.effect.Concurrent.ops.toAllConcurrentOps
import cats.effect.Timer
import cats.effect.concurrent.Ref
import cats.effect.concurrent.Semaphore
import cats.implicits._

import scala.concurrent.duration.DurationDouble
import scala.concurrent.duration.FiniteDuration

class RateLimiter[F[_]](
  rate: Double,
  window: FiniteDuration,
  timeAccumulated: Ref[F, FiniteDuration],
  allowParallel: Boolean,
  semaphore: Semaphore[F],
  resolution: Long
)(
  implicit concurrent: Concurrent[F],
  timer: Timer[F]
) extends CatsUtils[F] {

  val accumulationUnit = (window / rate).toNanos.nanos

  def execute[A](f: => F[A]) =
    for {
      waitTime <- timeAccumulated.modify(a => (a + accumulationUnit, a)) // add a unit to the accumulated wait time
      _ <- whenA(waitTime.length > 0)(sleep(waitTime) >> timeAccumulated.update(_ - waitTime)) // if previously accumulated time is positive, wait before starting execution
      _ <- semaphore.acquireN(resolution) // acquire the semaphore
      timer <- whenA(allowParallel)(sleep(accumulationUnit)).start // if parallel execution is allowed, start a timer that expires in unit time 
      execution <- f.start // start the execution
      _ <-
        if (allowParallel)
          (timer.join >> semaphore.releaseN(resolution)).start // if parallel execution is allowed, wait for the timer and then release the semaphore
        else
          execution.join >> semaphore.releaseN(resolution) // if parallel execution is not allowed, wait for the execution and then release the semaphore
      result <- execution.join
    } yield result
}

object RateLimiter {

  /**
   * Limits rate of evaluation of effects executed through this rate limiter to rate / window at a maximum.
   *
   * @param rate            The rate.
   * @param window          The time window. Default is 1 second.
   * @param timeAccumulated The time accumulated before the first execution. A positive time means the execution can start faster at the beginning. 
   *                        A negative time means execution has to be slower at the beginning because some previous executions are assumed to have happened (outside this rate limiter).
   *                        If not specified, the default assumption is that the first execution can start immediately.
   * @param allowParallel   Allow more than rate number of executions running at any time.
   *                        If true, only the rate of the start of execution is limited, irrespective of how long executions last.
   *                        If false, it is guaranteed that no more than rate number of executions run at any time. The default is true. 
   * @param resolution      The accuracy at which the rate is calculated. If the rate is an integer, it will be applied completely accurately. 
   *                        If the rate is not an integer, it will be rounded down to 1 / resolution.
   * @param concurrent
   * @param timer
   * @tparam F
   * @return
   */
  def apply[F[_]](
    rate: Double,
    window: FiniteDuration = 1.second,
    timeAccumulated: Option[FiniteDuration] = None,
    allowParallel: Boolean = true,
    resolution: Long = 1000
  )(
    implicit concurrent: Concurrent[F],
    timer: Timer[F]
  ) =
    for {
      semaphore <- Semaphore[F]((rate * resolution).toLong)
      timeAccumulated <- Ref.of(-timeAccumulated.getOrElse(0.nanos))
    } yield new RateLimiter(rate, window, timeAccumulated, allowParallel, semaphore, resolution)

}
