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

import quasar.connector.{DataEvent, MonadResourceErr, Offset}
import quasar.connector.destination.ResultSink.UpsertSink

import cats.Monad
import cats.implicits._

import fs2.{Pipe, Stream}

import org.slf4s.Logging

import skolems.Forall

// final case class Args[F[_], T, A](
//     path: ResourcePath,
//     columns: NonEmptyList[Column[T]],
//     correlationId: Column[TypedKey[T, A]],
//     input: Stream[F, DataEvent.Primitive[A, Offset]])
// }

object CsvUpsertSink extends Logging {

  // ResourcePath is used to extract the table name with `tableFromPath`
  // create Stream[F, Byte] and columns are passed to `copyToTable`
  // then we `createTable` with this information

  // TODO index the table at creation time by correlation id column

  def apply[F[_]: Monad: MonadResourceErr, T]
      : Forall[λ[α => UpsertSink.Args[F, T, α] => Stream[F, Offset]]] =
    Forall[λ[α => UpsertSink.Args[F, T, α] => Stream[F, Offset]]](run)

  def run[F[_]: Monad: MonadResourceErr, T, I](args: UpsertSink.Args[F, T, I])
      : Stream[F, Offset] = {

    val table: F[Table] = tableFromPath[F](args.path)

    // FIXME
    def handleCreate(create: DataEvent.Create): F[Unit] =
      ().pure[F]

    // FIXME
    def handleDelete(delete: DataEvent.Delete[I]): F[Unit] =
      ().pure[F]

    // FIXME
    def handleCommit(commit: DataEvent.Commit[Offset]): F[Offset] =
      commit.offset.pure[F]

    val eventHandler: Pipe[F, DataEvent.Primitive[I, Offset], Option[Offset]] =
      _ evalMap {
        case e @ DataEvent.Create(_) => handleCreate(e) >> (None: Option[Offset]).pure[F]
        case e @ DataEvent.Delete(_) => ??? // handleDelete(e) >> (None: Option[Offset]).pure[F]
        case e @ DataEvent.Commit(_) => handleCommit(e).map(Some(_): Option[Offset])
      }

    eventHandler(args.input).unNone
  }
}
