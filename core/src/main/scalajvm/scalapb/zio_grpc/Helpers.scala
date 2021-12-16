package scalapb.zio_grpc

import io.grpc.{Status}
import zio.{IO, ZIO}

object Helpers {
  def fromScalaPromise[T](promise: scala.concurrent.Promise[T]): IO[Status, T] = {
    ZIO.fromPromiseScala(promise).mapError[Status](err => Status.INTERNAL.withCause(err))
  }
  def fromScalaPromiseE[T](promise: scala.concurrent.Promise[Either[Status, T]]): IO[Status, T] = {
    ZIO.fromPromiseScala(promise).mapError[Status](err => Status.INTERNAL.withCause(err)).flatMap[Any, Status, T](_.fold(ZIO.fail(_), ZIO.succeed(_)))
  }
}
