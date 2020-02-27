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

import cats.Applicative

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

  def apply[F[_]: Applicative: MonadResourceErr, T]
      : Forall[λ[α => UpsertSink.Args[F, T, α] => Stream[F, Offset]]] =
    Forall[λ[α => UpsertSink.Args[F, T, α] => Stream[F, Offset]]](run)

  def run[F[_]: Applicative: MonadResourceErr, T, A](args: UpsertSink.Args[F, T, A])
      : Stream[F, Offset] = {

    val table: F[Table] = tableFromPath[F](args.path)

    val pipe: Pipe[F, DataEvent.Primitive[A, Offset], Unit] = ???

    ???
  }
}
