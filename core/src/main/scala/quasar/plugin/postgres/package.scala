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

package quasar.plugin

import slamdata.Predef._

import quasar.api.{Column, ColumnType}
import quasar.api.resource._
import quasar.connector.render.RenderConfig
import quasar.connector.{MonadResourceErr, ResourceError}

import java.net.URI
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, SignStyle}
import java.time.temporal.ChronoField

import scala.util.Random

import fs2.{Chunk, Pipe, Stream}

import cats.Applicative
import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect.{ExitCase, Sync}
import cats.effect.concurrent.Ref
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.postgres._
import doobie.postgres.implicits._
import doobie.util.log.{ExecFailure, ProcessingFailure, Success}

import org.slf4s.Logger

package object postgres {

  type Ident = String
  type Table = Ident

  val Redacted: String = "--REDACTED--"

  val PostgresCsvConfig: RenderConfig.Csv = {
    val eraPattern = " G"

    val time =
      DateTimeFormatter.ofPattern("HH:mm:ss.S")

    // mutable builder
    def unsignedDate =
      (new DateTimeFormatterBuilder())
        .appendValue(ChronoField.YEAR_OF_ERA, 4, 19, SignStyle.NEVER)
        .appendPattern("-MM-dd")

    RenderConfig.Csv(
      includeHeader = false,
      includeBom = false,

      offsetDateTimeFormat =
        unsignedDate
          .appendLiteral(' ')
          .append(time)
          .appendPattern("Z" + eraPattern)
          .toFormatter,

      localDateTimeFormat =
        unsignedDate
          .appendLiteral(' ')
          .append(time)
          .appendPattern(eraPattern)
          .toFormatter,

      localDateFormat =
        unsignedDate
          .appendPattern(eraPattern)
          .toFormatter)
  }

  /** Returns a quoted and escaped version of `ident`. */
  def hygienicIdent(ident: Ident): Ident =
    s""""${ident.replace("\"", "\"\"")}""""

  /** Returns the JDBC connection string corresponding to the given postgres URI. */
  def jdbcUri(pgUri: URI): String =
    s"jdbc:${pgUri}"

  /** Returns a random alphanumeric string of the specified length. */
  def randomAlphaNum[F[_]: Sync](size: Int): F[String] =
    Sync[F].delay(Random.alphanumeric.take(size).mkString)

  /** Attempts to extract a table name from the given path. */
  def tableFromPath[F[_]: Applicative: MonadResourceErr](path: ResourcePath)
      : F[Table] = {
    val back = Some(path) collect {
      case table /: ResourcePath.Root => table
    }

    back match {
      case Some(t) =>
        t.pure[F]
      case None =>
        MonadResourceErr[F].raiseError(ResourceError.notAResource(path))
    }
  }

  def logHandler(log: Logger): LogHandler =
    LogHandler {
      case Success(q, _, e, p) =>
        log.debug(s"SUCCESS: `$q` in ${(e + p).toMillis}ms (${e.toMillis} ms exec, ${p.toMillis} ms proc)")

      case ExecFailure(q, _, e, t) =>
        log.debug(s"EXECUTION_FAILURE: `$q` after ${e.toMillis} ms, detail: ${t.getMessage}", t)

      case ProcessingFailure(q, _, e, p, t) =>
        log.debug(s"PROCESSING_FAILURE: `$q` after ${(e + p).toMillis} ms (${e.toMillis} ms exec, ${p.toMillis} ms proc (failed)), detail: ${t.getMessage}", t)
    }

  def error[F[_]: Sync](log: Logger)(msg: => String, cause: => Throwable): F[Unit] =
    Sync[F].delay(log.error(msg, cause))

  def debug[F[_]: Sync](log: Logger)(msg: => String): F[Unit] =
    Sync[F].delay(log.debug(msg))

  def trace[F[_]: Sync](log: Logger)(msg: => String): F[Unit] =
    Sync[F].delay(log.trace(msg))

  def logChunkSize[F[_]: Sync](log: Logger)(c: Chunk[Byte]): F[Unit] =
    trace[F](log)(s"Sending ${c.size} bytes")

  def recordChunks[F[_]: Sync](log: Logger)(total: Ref[F, Long])(c: Chunk[Byte]): F[Unit] =
    total.update(_ + c.size) >> logChunkSize[F](log)(c)

  def copyToTable(
      log: Logger)(
      table: Table,
      columns: NonEmptyList[Column[ColumnType.Scalar]])
      : Pipe[CopyManagerIO, Chunk[Byte], Unit] = {
    val cols =
      columns
        .map(c => hygienicIdent(c.name))
        .intercalate(", ")

    val copyQuery =
      s"COPY ${hygienicIdent(table)} ($cols) FROM STDIN WITH (FORMAT csv, HEADER FALSE, ENCODING 'UTF8')"

    val logStart = debug[CopyManagerIO](log)(s"BEGIN COPY: `${copyQuery}`")

    val startCopy =
      Stream.bracketCase(PFCM.copyIn(copyQuery) <* logStart) { (pgci, exitCase) =>
        PFCM.embed(pgci, exitCase match {
          case ExitCase.Completed => PFCI.endCopy.void
          case _ => PFCI.isActive.ifM(PFCI.cancelCopy, PFCI.unit)
        })
      }

    in => startCopy flatMap { pgci =>
      in.map(_.toBytes) evalMap { bs =>
        PFCM.embed(pgci, PFCI.writeToCopy(bs.values, bs.offset, bs.length))
      }
    }
  }

  def createTable(log: Logger)(table: Table, colSpecs: NonEmptyList[Fragment]): ConnectionIO[Int] = {
    val preamble =
      fr"CREATE TABLE" ++ Fragment.const(hygienicIdent(table))

    (preamble ++ Fragments.parentheses(colSpecs.intercalate(fr",")))
      .updateWithLogHandler(logHandler(log))
      .run
  }

  def dropTableIfExists(log: Logger)(table: Table): ConnectionIO[Int] =
    (fr"DROP TABLE IF EXISTS" ++ Fragment.const(hygienicIdent(table)))
      .updateWithLogHandler(logHandler(log))
      .run

  def columnSpec(c: Column[ColumnType.Scalar]): ValidatedNel[ColumnType.Scalar, Fragment] =
    pgColumnType(c.tpe).map(Fragment.const(hygienicIdent(c.name)) ++ _)

  val pgColumnType: ColumnType.Scalar => ValidatedNel[ColumnType.Scalar, Fragment] = {
    case ColumnType.Null => fr0"smallint".validNel
    case ColumnType.Boolean => fr0"boolean".validNel
    case ColumnType.LocalTime => fr0"time".validNel
    case ColumnType.OffsetTime => fr0"time with timezone".validNel
    case ColumnType.LocalDate => fr0"date".validNel
    case t @ ColumnType.OffsetDate => t.invalidNel
    case ColumnType.LocalDateTime => fr0"timestamp".validNel
    case ColumnType.OffsetDateTime => fr0"timestamp with time zone".validNel
    case ColumnType.Interval => fr0"interval".validNel
    case ColumnType.Number => fr0"numeric".validNel
    case ColumnType.String => fr0"text".validNel
  }
}
