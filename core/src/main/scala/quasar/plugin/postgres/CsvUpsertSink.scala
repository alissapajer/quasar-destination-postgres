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

import quasar.connector.Offset
import quasar.connector.destination.ResultSink.UpsertSink

import fs2.Stream

import org.slf4s.Logging

import skolems.Forall

object CsvUpsertSink extends Logging {

  def back[F[_], T]: Forall[λ[α => UpsertSink.Args[F, T, α] => Stream[F, Offset]]] = ???
}
