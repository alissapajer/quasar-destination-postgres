/*
 * Copyright 2014–2020 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.plugin.postgres

import slamdata.Predef._

import cats._
import cats.arrow.FunctionK
import cats.data._
import cats.effect.{Effect, LiftIO, Timer}
import cats.effect.concurrent.Ref
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.postgres._
import doobie.postgres.implicits._

import fs2.Stream

import org.slf4s.Logging

import quasar.api.{Column, ColumnType}
import quasar.api.resource._
import quasar.connector._

import scala.concurrent.duration.MILLISECONDS

// TODO rename to CsvCreateSink
object CsvSink extends Logging {
  def apply[F[_]: Effect: MonadResourceErr](
      xa: Transactor[F],
      writeMode: WriteMode)(
      dst: ResourcePath,
      columns: NonEmptyList[Column[ColumnType.Scalar]],
      data: Stream[F, Byte])(
      implicit timer: Timer[F])
      : Stream[F, Unit] = {

    val AE = ApplicativeError[F, Throwable]

    val table: F[Table] = tableFromPath[F](dst)

    Stream.force(for {
      tbl <- table

      action = writeMode match {
        case WriteMode.Create => "Creating"
        case WriteMode.Replace => "Replacing"
      }

      _ <- debug[F](log)(s"${action} '${tbl}' with schema ${columns.show}")

      // Telemetry
      totalBytes <- Ref[F].of(0L)
      startAt <- timer.clock.monotonic(MILLISECONDS)

      doCopy =
        data
          .chunks
          .evalTap(recordChunks[F](log)(totalBytes))
          // TODO: Is there a better way?
          .translate(Effect.toIOK[F] andThen LiftIO.liftK[CopyManagerIO])
          .through(copyToTable(log)(tbl, columns))

      colSpecs <- columns.traverse(columnSpec).fold(
        invalid => AE.raiseError(new ColumnTypesNotSupported(invalid)),
        _.pure[F])

      ensureTable =
        writeMode match {
          case WriteMode.Create =>
            createTable(log)(tbl, colSpecs)

          case WriteMode.Replace =>
            dropTableIfExists(log)(tbl) >> createTable(log)(tbl, colSpecs)
        }

      copy0 =
        Stream.eval(ensureTable).void ++
          doCopy.translate(λ[FunctionK[CopyManagerIO, ConnectionIO]](PHC.pgGetCopyAPI(_)))

      copy = copy0.transact(xa) handleErrorWith { t =>
        Stream.eval(
          error[F](log)(s"COPY to '${tbl}' produced unexpected error: ${t.getMessage}", t) >>
            AE.raiseError(t))
      }

      logEnd = for {
        endAt <- timer.clock.monotonic(MILLISECONDS)
        tbytes <- totalBytes.get
        _ <- debug[F](log)(s"SUCCESS: COPY ${tbytes} bytes to '${tbl}' in ${endAt - startAt} ms")
      } yield ()


    } yield copy ++ Stream.eval(logEnd))
  }
}
