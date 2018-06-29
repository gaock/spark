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



class RiffleShuffleSuite extends SparkFunSuite with LocalSparkContext {

  // This test suite should run all tests in ShuffleSuite with sort-based shuffle.


  override def beforeAll() {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
        .set("spark.conf.isUseRiffle", "true")
        .set("spark.conf.riffleThreshold", "10")
        .set("spark.conf.readSize", "30")
    sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }
//  test("not use riffle") {
//    sc.parallelize(1 to 1000, 10).map((_, 1)).reduceByKey(_ + _).count
//  }
  test("use riffle") {
    sc.setLogLevel("INFO")
    val res = sc.parallelize(1 to 2000000, 10).map(key => (key % 100, 1))
      .reduceByKey(_ + _, 10).count
    // scalastyle:off  println
    println("\n****" + res)
    // scalastyle:on  println
  }
}
