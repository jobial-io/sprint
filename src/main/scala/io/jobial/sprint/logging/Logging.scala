package io.jobial.sprint.logging

import cats.effect.Sync
import com.typesafe.scalalogging.LazyLogging


trait Logging[F[_]] extends LazyLogging {

  def trace(msg: => String)(implicit sync: Sync[F]) = Sync[F].delay(logger.trace(msg))

  def trace(msg: => String, t: Throwable)(implicit sync: Sync[F]) = Sync[F].delay(logger.trace(msg, t))

  def debug(msg: => String)(implicit sync: Sync[F]) = Sync[F].delay(logger.debug(msg))

  def debug(msg: => String, t: Throwable)(implicit sync: Sync[F]) = Sync[F].delay(logger.debug(msg, t))

  def info(msg: => String)(implicit sync: Sync[F]) = Sync[F].delay(logger.info(msg))

  def info(msg: => String, t: Throwable)(implicit sync: Sync[F]) = Sync[F].delay(logger.info(msg, t))

  def warn(msg: => String)(implicit sync: Sync[F]) = Sync[F].delay(logger.warn(msg))

  def warn(msg: => String, t: Throwable)(implicit sync: Sync[F]) = Sync[F].delay(logger.warn(msg, t))

  def error(msg: => String)(implicit sync: Sync[F]) = Sync[F].delay(logger.error(msg))

  def error(msg: => String, t: Throwable)(implicit sync: Sync[F]) = Sync[F].delay(logger.error(msg, t))
}