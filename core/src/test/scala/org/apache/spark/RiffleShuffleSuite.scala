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
        .set("spark.conf.readSize", "300")
    sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }
  test("use riffle") {
    val res = sc.parallelize(1 to 1000, 11).map(key => (key % 25, 1)).sortByKey().collect()
    // scalastyle:off  println
    res.foreach(println)
  }
//  test("use riffle") {
//    val time1 = System.currentTimeMillis()
//    val res = sc.parallelize(1 to 2000000, 2100).map(key => (key % 1000, 1))
//      .reduceByKey(_ + _, 2100).count
//    val res = sc.parallelize(1 to 2000).map(key => (key % 100, 1))
//      .reduceByKey(_ + _).count
//    val time2 = System.currentTimeMillis()
//    val res2 = sc.parallelize(1 to 2000).map(key => (key % 100, 1))
//      .sortByKey()
//    res2.count()
//    val time3 = System.currentTimeMillis()
//    val res3 = sc.parallelize(1 to 1888).map(key => (key, 1))
//      .sortByKey()
//    res3.count()
//    val time4 = System.currentTimeMillis()
//    // scalastyle:off  println
//    println("----------------------time------------------")
//    println("time1" + time1)
//    println("time2" + time2)
//    println("time3" + time3)
//    println("***1***" + (time2 - time1))
//    println("***2***" + (time3 - time2))
//    println("***3***" + (time4 - time3))
//    println("----------------------time------------------")
//    println("\n****" + res)
//    println("*******************************************************")
//    res2.foreach(println)
//    println("*******************************************************")
//    res3.foreach(println)
//    println(System.currentTimeMillis() - time1)
//    // scalastyle:on  println
//  }
}
