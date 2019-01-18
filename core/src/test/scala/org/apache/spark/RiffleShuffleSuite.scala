/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import org.apache.commons.lang3.RandomStringUtils

import scala.util.Random


class RiffleShuffleSuite extends SparkFunSuite with LocalSparkContext {

  // This test suite should run all tests in ShuffleSuite with sort-based shuffle.


  override def beforeAll() {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
        .set("spark.conf.isUseRiffle", "true")
        .set("spark.conf.riffleThreshold", "10")
        .set("spark.conf.readSize", "300")
      .set("spark.conf.errorRate", "0.1")
    sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }
  test("use riffle") {
//    val random = Random
//    random.nextInt()
    val res = sc.parallelize(1 to 1000, 25).map(key => (key % 25, 1)).sortByKey(false).collect()
    // scalastyle:off  println
    res.foreach(println)
  }
}
