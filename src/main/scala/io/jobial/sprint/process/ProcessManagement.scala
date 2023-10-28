package io.jobial.sprint.process

import cats.effect.Concurrent
import cats.effect.Sync
import cats.effect.Timer
import cats.implicits._
import io.jobial.sprint.logging.Logging
import io.jobial.sprint.process.ProcessContext.sysEnv
import io.jobial.sprint.util._
import org.apache.commons.io.IOUtils

import java.io.File
import java.util.concurrent.TimeoutException
import scala.collection.JavaConverters._
import scala.concurrent.duration._

case class ProcessInfo[F[_] : Sync](
  process: Process,
  commandLine: List[String]
) extends CatsUtils[F] {
  def getOutput = delay(IOUtils.toString(process.getInputStream))
}

case class ProcessContext(
  directory: Option[String] = None,
  inputFilename: Option[String] = None,
  outputFilename: Option[String] = None,
  errorFilename: Option[String] = None,
  environment: Map[String, String] = sysEnv,
  timeout: FiniteDuration = 30.minutes,
  // This flag has been added to avoid the dangerous design in ProcessBuilder, which redirects to pipes by default, 
  // causing random hanging. See also https://stackoverflow.com/questions/3285408/java-processbuilder-resultant-process-hangs
  keepOutput: Boolean = false,
  inheritIO: Boolean = false
)

object ProcessContext {
  val sysEnv = sys.env
}

trait ProcessManagement[F[_]] extends CatsUtils[F] with Logging[F] {

  implicit def processInfoToProcess(processInfo: ProcessInfo[F]) = processInfo.process

  implicit def processContext = ProcessContext()

  def waitForProcessExit(process: ProcessInfo[F], timeout: FiniteDuration)(implicit concurrent: Concurrent[F], timer: Timer[F]) = {
    def waitForProcessExit(timeout: FiniteDuration, retry: Double): F[ProcessInfo[F]] =
      if (retry > 0)
        sleep(timeout) >> {
          if (process.isAlive)
            waitForProcessExit(timeout, retry - 1)
          else pure(process)
        }
      else raiseError(new TimeoutException(s"timed out waiting for $process"))

    debug(s"waiting for $process for ${timeout.toMillis}") >>
      waitForProcessExit(100.millis, timeout / 100.millis)
  }

  def kill(args: String*)(implicit processContext: ProcessContext, concurrent: Concurrent[F], timer: Timer[F]) =
    runProcess("/bin/kill" +: args)

  val defaultKillTimeout = 5.seconds

  def killProcess(process: ProcessInfo[F], signal: String = "-TERM", timeout: FiniteDuration = defaultKillTimeout, sendSigKillIfNotExited: Boolean = true)
    (implicit processContext: ProcessContext, concurrent: Concurrent[F], timer: Timer[F]): F[ProcessInfo[F]] =
    kill(signal, process.pid.toString) >>
      waitForProcessExit(process, timeout).onError { case t =>
        whenA(sendSigKillIfNotExited) {
          killProcess(process, "-KILL", timeout, sendSigKillIfNotExited = false)
        }
      } >> pure(process)

  def runProcess(command: Seq[String])(implicit processContext: ProcessContext, concurrent: Concurrent[F], timer: Timer[F]) =
    for {
      process <- delay {
        val builder = new ProcessBuilder(command: _*)
        processContext.directory.map(d => builder.directory(new File(d)))
        if (processContext.inheritIO)
          builder.inheritIO()
        else {
          processContext.inputFilename.map(f => builder.redirectInput(new File(f)))
          processContext.outputFilename.map(f => builder.redirectOutput(new File(f))).getOrElse {
            if (!processContext.keepOutput) builder.redirectOutput(new File("/dev/null"))
          }
          processContext.errorFilename.map(f => builder.redirectError(new File(f))).getOrElse {
            if (!processContext.keepOutput) builder.redirectError(new File("/dev/null"))
          }
        }
        if (processContext.environment ne sysEnv) {
          builder.environment.clear
          builder.environment.putAll(processContext.environment.asJava)
        }
        builder.start
      }
      _ <- debug(s"started $process for ${command.mkString(" ")}")
    } yield ProcessInfo(process, command.toList)

  def waitForProcessOrKill(process: ProcessInfo[F])
    (implicit processContext: ProcessContext, concurrent: Concurrent[F], timer: Timer[F]) =
    waitForProcessExit(process, processContext.timeout).handleErrorWith { t =>
      killProcess(process)
    } >> {
      if (process.exitValue != 0)
        raiseError(ProcessNonZeroExitStatus(process))
      else
        pure(process)
    }

  val maxProcessWaitTimeout = 1.day

  def runProcessAndWait(command: Seq[String])
    (implicit processContext: ProcessContext, concurrent: Concurrent[F], timer: Timer[F]): F[ProcessInfo[F]] =
    for {
      p <- runProcess(command)
      r <- waitForProcessOrKill(p)
    } yield r

  def sync(args: String*)(implicit processContext: ProcessContext, concurrent: Concurrent[F], timer: Timer[F]) =
    runProcessAndWait("/usr/bin/sync" +: args)

  def rm(args: String*)(implicit processContext: ProcessContext, concurrent: Concurrent[F], timer: Timer[F]) =
    runProcessAndWait("/usr/bin/rm" +: args)

  def mv(args: String*)(implicit processContext: ProcessContext, concurrent: Concurrent[F], timer: Timer[F]) =
    runProcessAndWait("/usr/bin/mv" +: args)

  def cp(args: String*)(implicit processContext: ProcessContext, concurrent: Concurrent[F], timer: Timer[F]) =
    runProcessAndWait("/usr/bin/cp" +: args)

  def mkdir(args: String*)(implicit processContext: ProcessContext, concurrent: Concurrent[F], timer: Timer[F]) =
    runProcessAndWait("/usr/bin/mkdir" +: args)

  def touch(args: String*)(implicit processContext: ProcessContext, concurrent: Concurrent[F], timer: Timer[F]) =
    runProcessAndWait("/usr/bin/touch" +: args)

  def du(args: String*)(implicit processContext: ProcessContext, concurrent: Concurrent[F], timer: Timer[F]) =
    runProcessAndWait("/usr/bin/du" +: args)
}

case class ProcessNonZeroExitStatus[F[_]](process: ProcessInfo[F])
  extends RuntimeException(s"process exited with non-zero status: $process")