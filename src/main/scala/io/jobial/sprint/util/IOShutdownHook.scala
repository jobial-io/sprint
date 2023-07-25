package io.jobial.sprint.util

import cats.effect.IO
import cats.effect.Sync
import io.jobial.sprint.logging.Logging

trait IOShutdownHook extends CatsUtils[IO] with Logging[IO] {

  def run: IO[Unit]

  def add[F[_]](implicit F: Sync[F]) =
    F.delay {
      sys.addShutdownHook(run.unsafeRunSync())
    }
}
