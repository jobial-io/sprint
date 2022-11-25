package io.jobial.sprint.process

import cats.effect.Sync
import cats.implicits._
import io.jobial.sprint.logging.Logging
import io.jobial.sprint.process.ProcessContext.sysEnv
import io.jobial.sprint.util._
import org.apache.commons.io.IOUtils

import java.io.File
import java.util.concurrent.TimeoutException
import scala.collection.convert.ImplicitConversions.`map AsScala`
import scala.concurrent.duration._

case class ProcessInfo[F[_] : Sync](
  process: Process,
  commandLine: List[String]
) extends CatsUtils[F] {
  def getOutput = delay(IOUtils.toString(process.getInputStream))
}

case class ProcessContext(
  directory: Option[String] = None,
  outputFilename: Option[String] = None,
  errorFilename: Option[String] = None,
  environment: Map[String, String] = sysEnv,
  // This flag has been added to avoid the dangerous design in ProcessBuilder that redirects to pipes by default, 
  // causing random hanging. See also https://stackoverflow.com/questions/3285408/java-processbuilder-resultant-process-hangs
  keepOutput: Boolean = false
)

object ProcessContext {
  val sysEnv = sys.env
}

trait ProcessManagement[F[_]] extends CatsUtils[F] with Logging[F] {

  implicit def processInfoToProcess(processInfo: ProcessInfo[F]) = processInfo.process

  implicit def processContext = ProcessContext()

  def waitForProcessExit(process: ProcessInfo[F], timeout: FiniteDuration)(implicit temporal: TemporalEffect[F]) = {
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

  def kill(args: String*)(implicit processContext: ProcessContext, temporal: TemporalEffect[F]) =
    runProcess("/bin/kill" +: args)

  val defaultKillTimeout = 5.seconds

  def killProcess(process: ProcessInfo[F], signal: String = "-TERM", timeout: FiniteDuration = defaultKillTimeout, sendSigKillIfNotExited: Boolean = true)
    (implicit processContext: ProcessContext, temporal: TemporalEffect[F]): F[ProcessInfo[F]] =
    kill(signal, process.pid.toString) >>
      waitForProcessExit(process, timeout).onError { t =>
        whenA(sendSigKillIfNotExited) {
          killProcess(process, "-KILL", timeout, sendSigKillIfNotExited = false)
        }
      } >> pure(process)

  def runProcess(command: Seq[String])(implicit processContext: ProcessContext, temporal: TemporalEffect[F]) =
    for {
      process <- delay {
        val builder = new ProcessBuilder(command: _*)
        processContext.directory.map(d => builder.directory(new File(d)))
        processContext.outputFilename.map(f => builder.redirectOutput(new File(f))).getOrElse {
          if (!processContext.keepOutput) builder.redirectOutput(new File("/dev/null"))
        }
        processContext.errorFilename.map(f => builder.redirectError(new File(f))).getOrElse {
          if (!processContext.keepOutput) builder.redirectError(new File("/dev/null"))
        }
        if (processContext.environment ne sysEnv) {
          builder.environment.clear
          builder.environment ++= processContext.environment
        }
        builder.start
      }
      _ <- debug(s"started $process for ${command.mkString(" ")}")
    } yield ProcessInfo(process, command.toList)

  def waitForProcessOrKill(process: ProcessInfo[F], timeout: FiniteDuration)
    (implicit processContext: ProcessContext, temporal: TemporalEffect[F]) =
    waitForProcessExit(process, timeout).handleErrorWith { t =>
      killProcess(process)
    } >> {
      if (process.exitValue != 0)
        raiseError(ProcessNonZeroExitStatus(process))
      else
        pure(process)
    }

  val maxProcessWaitTimeout = 1.day

  def runProcessAndWait(command: Seq[String], timeout: FiniteDuration = maxProcessWaitTimeout)
    (implicit processContext: ProcessContext, temporal: TemporalEffect[F]): F[ProcessInfo[F]] =
    for {
      p <- runProcess(command)
      r <- waitForProcessOrKill(p, timeout)
    } yield r

  def sync(args: String*)(implicit processContext: ProcessContext, temporal: TemporalEffect[F]) =
    runProcessAndWait("/usr/bin/sync" +: args)

  def rm(args: String*)(implicit processContext: ProcessContext, temporal: TemporalEffect[F]) =
    runProcessAndWait("/usr/bin/rm" +: args)

  def mv(args: String*)(implicit processContext: ProcessContext, temporal: TemporalEffect[F]) =
    runProcessAndWait("/usr/bin/mv" +: args)

  def cp(args: String*)(implicit processContext: ProcessContext, temporal: TemporalEffect[F]) =
    runProcessAndWait("/usr/bin/cp" +: args)

  def mkdir(args: String*)(implicit processContext: ProcessContext, temporal: TemporalEffect[F]) =
    runProcessAndWait("/usr/bin/mkdir" +: args)

  def touch(args: String*)(implicit processContext: ProcessContext, temporal: TemporalEffect[F]) =
    runProcessAndWait("/usr/bin/touch" +: args)

  def du(args: String*)(implicit processContext: ProcessContext, temporal: TemporalEffect[F]) =
    runProcessAndWait("/usr/bin/du" +: args)
}

case class ProcessNonZeroExitStatus[F[_]](process: ProcessInfo[F])
  extends RuntimeException(s"process exited with non-zero status: $process")