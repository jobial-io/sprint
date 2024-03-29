package io.jobial.sprint.util

import cats.effect._
import cats.effect.concurrent.MVar
import cats.implicits._
import cats.Applicative
import cats.Monad
import cats.MonadError
import cats.Parallel
import cats.Traverse

import java.util.concurrent.CancellationException
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import scala.Console.RESET
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.Future
import scala.concurrent.TimeoutException
import scala.language.postfixOps
import scala.util.Failure
import scala.util.Success

trait CatsUtils[F[_]] {

  def whenA[A](cond: Boolean)(f: => F[A])(implicit F: Monad[F]): F[Unit] =
    if (cond) F.void(f) else F.unit

  // Monad would be enough here but Sync avoids some implicit clashes without causing any restrictions in practice
  def unit(implicit F: Monad[F]) = F.unit

  // Monad would be enough here but Sync avoids some implicit clashes without causing any restrictions in practice
  def pure[A](a: A)(implicit F: Monad[F]) = F.pure(a)

  type MonadErrorWithThrowable[F[_]] = MonadError[F, Throwable]

  def raiseError[A](t: Throwable)(implicit F: MonadErrorWithThrowable[F]) = F.raiseError[A](t)

  def delay[A](f: => A)(implicit F: Sync[F]) = F.delay(f)

  def defer[A](f: => F[A])(implicit F: Sync[F]) = Sync[F].defer(f)

  def liftIO[A](f: IO[A])(implicit F: LiftIO[F]) = F.liftIO(f)

  def sleep(duration: FiniteDuration)(implicit F: Timer[F]) = F.sleep(duration)

  def start[A](f: F[A])(implicit F: Concurrent[F]) = F.start(f)

  def never[A](implicit F: Async[F]) = F.never[A]

  def fromFuture[A](f: => Future[A])(implicit F: Concurrent[F]): F[A] = {
    delay(f).flatMap { f =>
      f.value match {
        case Some(result) =>
          result match {
            case Success(a) => pure(a)
            case Failure(e) => raiseError(e)
          }
        case _ =>
          Concurrent[F].async { cb =>
            f.onComplete { r =>
              cb(r match {
                case Success(a) => Right(a)
                case Failure(e) => Left(e)
              })
            }(blockerContext)
          }
      }
    }
  }

  def fromEither[A](e: Either[Throwable, A])(implicit F: MonadErrorWithThrowable[F]): F[A] =
    e match {
      case Right(a) => pure[A](a)
      case Left(err) => raiseError(err)
    }

  def fromJavaFuture[A](future: => java.util.concurrent.Future[A], pollTime: FiniteDuration = 10.millis)(implicit sync: Sync[F]): F[A] =
    for {
      f <- delay(future)
      r <- delay(f.get(pollTime.toMillis, TimeUnit.MILLISECONDS)).handleErrorWith {
        case t: CancellationException =>
          raiseError(t)
        case t: ExecutionException =>
          raiseError(t.getCause)
        case _ =>
          fromJavaFuture(f, pollTime)
      }
    } yield r

  def waitFor[A](f: => F[A])(cond: A => F[Boolean], pollTime: FiniteDuration = 1.second)(implicit concurrent: Concurrent[F], timer: Timer[F]): F[A] =
    for {
      a <- f
      c <- cond(a)
      r <- if (c) pure(a) else sleep(pollTime) >> waitFor(f)(cond, pollTime)
    } yield r

  case class IterableSequenceSyntax[T](l: Iterable[F[T]])(implicit parallel: Parallel[F], applicative: Applicative[F]) {

    def parSequence = Parallel.parSequence(l.toList)

    def sequence = Traverse[List].sequence(l.toList)
  }

  implicit def iterableToSequenceSyntax[T](l: Iterable[F[T]])(implicit parallel: Parallel[F], applicative: Applicative[F]) =
    IterableSequenceSyntax(l)

  def take[T](mvar: MVar[F, T], timeout: Option[FiniteDuration], pollTime: FiniteDuration = 1.millis)(implicit concurrent: Concurrent[F], timer: Timer[F]): F[T] =
    timeout match {
      case Some(timeout) =>
        for {
          r <- mvar.tryTake
          r <- r match {
            case Some(r) =>
              pure(r)
            case None =>
              if (timeout > 0.millis)
                sleep(pollTime) >>
                  take(mvar, Some(timeout - pollTime))
              else raiseError(new TimeoutException)
          }
        } yield r
      case None =>
        mvar.take
    }

  def guarantee[A](fa: F[A])(finalizer: F[Unit])(implicit bracket: Bracket[F, Throwable]): F[A] =
    Bracket[F, Throwable].guarantee(fa)(finalizer)

  def printLn(msg: String)(implicit sync: Sync[F]) =
    delay(println(msg))

  def printStr(msg: String)(implicit sync: Sync[F]) =
    delay(print(msg))

  def printColorLn(msg: String)(implicit sync: Sync[F]) =
    printLn(s"${RESET}${msg}${RESET}")
}

object CatsUtils {

  def apply[F[_]] = new CatsUtils[F] {}
}