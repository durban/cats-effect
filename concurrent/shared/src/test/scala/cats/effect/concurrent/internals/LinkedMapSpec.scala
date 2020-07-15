/*
 * Copyright 2020 Typelevel
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

package cats.effect.internals

import org.specs2.mutable.Specification

final class LinkedMapSpec extends Specification {

  "linked map" should {

    "empty map" in {
      val map = LinkedMap.empty[Int, Int]

      map.isEmpty must beTrue
    }

    "inserting entries" in {
      val ns = (0 until 10).toList
      val map = ns.foldLeft(LinkedMap.empty[Int, Int])((map, i) => map.updated(i, i))

      map.isEmpty must beEqualTo(false)
      map.keys.toList must beEqualTo(ns)
      map.values.toList must beEqualTo(ns)
    }

    "dequeueing entries" in {
      val ns = (0 until 10).toList
      val map = ns.foldLeft(LinkedMap.empty[Int, Int])((map, i) => map.updated(i, i))

      var n = 0
      var acc = map
      while (!acc.isEmpty) {
        val res = acc.dequeue

        res._1 must beEqualTo(n)

        n += 1
        acc = res._2
      }

      acc.isEmpty must beTrue
    }

    "removing entry" in {
      val ns = (0 until 10).toList
      val map = ns.foldLeft(LinkedMap.empty[Int, Int])((map, i) => map.updated(i, i))
      val n = 2

      assert(map.keys.exists(_ == n))
      assert(map.values.exists(_ == n))

      map.keys.exists(_ == n) must beEqualTo(true)
      map.values.exists(_ == n) must beEqualTo(true)

      val map2 = map - n

      map2.keys.exists(_ == n) must beFalse
      map2.values.exists(_ == n) must beFalse
    }

  }

}