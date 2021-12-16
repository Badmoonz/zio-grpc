package scalapb.zio_grpc.client

import scalapb.zio_grpc.Helpers._
import zio.{IO, Ref, Runtime, UIO, ZIO}
import io.grpc.ClientCall
import io.grpc.{Metadata, Status}
import UnaryCallState._

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Promise

sealed trait UnaryCallState[+Res]

object UnaryCallState {
  case object Initial extends UnaryCallState[Nothing]

  case class HeadersReceived[Res](headers: Metadata) extends UnaryCallState[Res]

  case class ResponseReceived[Res](headers: Metadata, message: Res) extends UnaryCallState[Res]

  case class Failure[Res](s: String) extends UnaryCallState[Res]
}

class UnaryClientCallListener[Res](
  runtime: Runtime[Any],
) extends ClientCall.Listener[Res] {
  private val state = new AtomicReference[UnaryCallState[Res]]()
  private val promise = Promise[Either[Status, (Metadata, Res)]]()

  override def onHeaders(headers: Metadata): Unit = {
    state.updateAndGet({
      case Initial                => HeadersReceived(headers)
      case HeadersReceived(_)     => Failure("onHeaders already called")
      case ResponseReceived(_, _) => Failure("onHeaders already called")
      case f @ Failure(_)         => f
    })
  }


  override def onMessage(message: Res): Unit = {
    state.updateAndGet({
      case Initial                  => Failure("onMessage called before onHeaders")
      case HeadersReceived(headers) => ResponseReceived(headers, message)
      case ResponseReceived(_, _)   =>
        Failure("onMessage called more than once for unary call")
      case f @ Failure(_)           => f
    })
  }

  override def onClose(status: Status, trailers: Metadata): Unit = {
    if (!status.isOk) promise.success(Left(status))
    else
      state.get() match {
        case ResponseReceived(headers, message) =>
          promise.success(Right((headers, message)))
        case Failure(errorMessage)              =>
          promise.success(Left(Status.INTERNAL.withDescription(errorMessage)))
        case _                                  =>
          promise.success(Left(Status.INTERNAL.withDescription("No data received")))
      }
  }

  def getValue: IO[Status, (Metadata, Res)] = fromScalaPromiseE(promise)
}

object UnaryClientCallListener {
  def make[Res] =
    for {
      runtime <- zio.ZIO.runtime[Any]
    } yield new UnaryClientCallListener[Res](runtime)
}
