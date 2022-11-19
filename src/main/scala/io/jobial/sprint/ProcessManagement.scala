package io.jobial.sprint

import cats.effect.IO
import cats.implicits.catsSyntaxFlatMapOps
import io.jobial.sprint.ProcessContext.sysEnv
import io.jobial.sprint.logging.Logging
import io.jobial.sprint.util.TemporalEffect
import org.apache.commons.io.IOUtils

import java.io.File
import java.util.concurrent.TimeoutException
import scala.collection.convert.ImplicitConversions.`map AsScala`
import scala.concurrent.duration._
import io.jobial.sprint.util._

case class ProcessInfo(
  process: Process,
  commandLine: List[String]
) {
  def getOutput = IO(IOUtils.toString(process.getInputStream))
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

trait ProcessManagement
  extends Logging {

  implicit def processInfoToProcess(processInfo: ProcessInfo) = processInfo.process

  implicit def processContext = ProcessContext()

  def waitForProcessExit(process: ProcessInfo, timeout: FiniteDuration)(implicit temporal: TemporalEffect[IO]) = {
    def waitForProcessExit(timeout: FiniteDuration, retry: Double): IO[ProcessInfo] =
      if (retry > 0)
        sleep(timeout) >> {
          if (process.isAlive)
            waitForProcessExit(timeout, retry - 1)
          else IO(process)
        }
      else raiseError(new TimeoutException(s"timed out waiting for $process"))

    debug(s"waiting for $process for ${timeout.toMillis}") >>
      waitForProcessExit(100.millis, timeout / 100.millis)
  }

  def kill(args: String*)(implicit processContext: ProcessContext) =
    runProcess("/bin/kill" +: args)

  val defaultKillTimeout = 5.seconds

  def killProcess(process: ProcessInfo, signal: String = "-TERM", timeout: FiniteDuration = defaultKillTimeout, sendSigKillIfNotExited: Boolean = true)
    (implicit processContext: ProcessContext, temporal: TemporalEffect[IO]): IO[ProcessInfo] =
    kill(signal, process.pid.toString) >>
      waitForProcessExit(process, timeout).handleErrorWith { t =>
        whenA(sendSigKillIfNotExited) {
          killProcess(process, "-KILL", timeout, sendSigKillIfNotExited = false)
        }
      } >> IO(process)

  def runProcess(command: Seq[String])(implicit processContext: ProcessContext) =
    for {
      process <- IO {
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
      _ <- debug[IO](s"started $process for ${command.mkString(" ")}")
    } yield ProcessInfo(process, command.toList)

  def waitForProcessOrKill(process: ProcessInfo, timeout: FiniteDuration)(implicit processContext: ProcessContext) =
    waitForProcessExit(process, timeout).handleErrorWith { t =>
      killProcess(process)
    } >> {
      if (process.exitValue != 0)
        IO.raiseError(ProcessNonZeroExitStatus(process))
      else
        IO(process)
    }

  val maxProcessWaitTimeout = 1.day

  def runProcessAndWait(command: Seq[String], timeout: FiniteDuration = maxProcessWaitTimeout)(implicit processContext: ProcessContext): IO[ProcessInfo] =
    runProcess(command) >>=
      (waitForProcessOrKill(_, timeout))

  def sync(args: String*)(implicit processContext: ProcessContext) =
    runProcessAndWait("/usr/bin/sync" +: args)

  def rm(args: String*)(implicit processContext: ProcessContext) =
    runProcessAndWait("/usr/bin/rm" +: args)

  def mv(args: String*)(implicit processContext: ProcessContext) =
    runProcessAndWait("/usr/bin/mv" +: args)

  def cp(args: String*)(implicit processContext: ProcessContext) =
    runProcessAndWait("/usr/bin/cp" +: args)

  def mkdir(args: String*)(implicit processContext: ProcessContext) =
    runProcessAndWait("/usr/bin/mkdir" +: args)

  def touch(args: String*)(implicit processContext: ProcessContext) =
    runProcessAndWait("/usr/bin/touch" +: args)

  def du(args: String*)(implicit processContext: ProcessContext) =
    runProcessAndWait("/usr/bin/du" +: args)
}

case class ProcessNonZeroExitStatus(process: ProcessInfo)
  extends RuntimeException(s"process exited with non-zero status: $process")