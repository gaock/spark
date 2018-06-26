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

package org.apache.spark.shuffle.sort

import java.io.{BufferedOutputStream, DataOutputStream, FileOutputStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.storage.{BlockId, DiskBlockObjectWriter, ShuffleBlockId, ShuffleIndexBlockId}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.ExternalSorter






private[spark] class SortShuffleWriter[K, V, C](
   shuffleBlockResolver: IndexShuffleBlockResolver,
   handle: BaseShuffleHandle[K, V, C],
   mapId: Int,
   context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency

  private val blockManager = SparkEnv.get.blockManager

  private var sorter: ExternalSorter[K, V, _] = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = null

  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics
  //  var ReadSegmentTimes = new ArrayBuffer[Int]()
  private var memoryByte = Map[(Int, ShuffleBlockId), Array[Byte]]()
  private var blockIdRead = Map[ShuffleBlockId, (Long, Boolean, Array[Long])]()
  private var readResult = Map[ShuffleBlockId, Array[Byte]]()
  private var readSegmentInfo = Map[ShuffleBlockId, (Int, Int)]()
  private var partitionLengths = new Array[Long](1)
  private var rifflePartitionLengths = new Array[Long](2)
  private var segmentStatuses = new mutable.HashMap[(ShuffleBlockId, Int), (Boolean, Array[Byte])]()

  private var mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker
  private var mergeBlocksLengths = 0
  private val conf = SparkEnv.get.conf
  private val isUseRiffle = conf.getBoolean("spark.conf.isUseRiffle", false)
  private val readSize = conf.getInt("spark.conf.readSize", 1024*1024*1)
  private val fileBufferSize = conf.getSizeAsKb("spark.conf.riffleBuffer", "32k").toInt * 1024
  private val riffleThreshold = conf.getInt("spark.conf.riffleThreshold", 40)

  /* spark.conf.isUseRiffle,false
  *  spark.conf.riffleThreshold,40
  *  spark.conf.riffleBuffer,32k
  *  spark.conf.readSize,1024*1024*1
  *
  * */

  /** Write a bunch of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    sorter = if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    sorter.insertAll(records)
    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    val tmp = Utils.tempFileWith(output)
    try {
      val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
      partitionLengths = sorter.writePartitionedFile(blockId, tmp)
      shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
      }
    }
    if (isUseRiffle) {
      if (mapId == 9) {
        val blockTest = blockManager.getMatchingBlockIds(_.isShuffleData)
        print("\n ----------block test---------------------------\n")
        for (id <- blockTest) {
          print("\n" + id.name)
        }
        print("\n ----------block test---------------------------\n")
      }
      rifflePartitionLengths = new Array[Long](partitionLengths.length)
      val res = isRiffleMerge()
      if (res._1) {
        logInfo(s"We'll start merge files****taskId=$mapId")
        getRiffleInfo(res._2)
        //  Use only one writer to write this riffle file
        // (shuffleId,mapId,reduceId(100).riffleData/riffleIndex)
        //  output = shuffleBlockResolver.getRiffleDataFile(dep.shuffleId, mapId)
        val output = shuffleBlockResolver.getRiffleDataFile(dep.shuffleId, mapId)
        val tmp = Utils.tempFileWith(output)
        val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tmp)))
        while (rifflePartitionLengths.last == 0) {
          readBlock()
          mergeRiffleBlocks()
          writeToDisk(out, res._2)
        }
        out.close()
        shuffleBlockResolver.writeRiffleIndexFileAndCommit(dep.shuffleId, mapId,
          rifflePartitionLengths, tmp)
        print("\n-------------------rifflePartitionLength---------------------\n")
        rifflePartitionLengths.foreach(print)
        mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths,
          rifflePartitionLengths, res._2.toArray)
      } else {
        logInfo(s"waiting threshold files***taskId=$mapId")
      }
      if (mapId == 9) {
        val blockTest = blockManager.getMatchingBlockIds(_.isShuffleData)
        print("\n ----------block test---------------------------\n")
        for (id <- blockTest) {
          print("\n" + id.name)
        }
        print("\n ----------block test---------------------------\n")
      }
    }
  }
// success
  def isRiffleMerge(): (Boolean, Seq[ShuffleBlockId]) = {
    while (true) {
      if (blockManager.status) {
        synchronized {
          blockManager.occupy
          val riffleBlocks = new ArrayBuffer[ShuffleBlockId]()
          val blockInfos = blockManager.getTaskResultInfos()
          for ((shuffleBlockId, falg) <- blockInfos) {
            if (!falg) {
              riffleBlocks.append(shuffleBlockId)
            }
          }
          // insert itself
          riffleBlocks.append(ShuffleBlockId(dep.shuffleId, mapId, 0))
          if (riffleBlocks.length >= riffleThreshold ||
            blockInfos.iterator.length == dep.partitioner.numPartitions - 1) {
            for (id <- riffleBlocks) {
              // scalastyle:off  println
              println(s"riffle blocks--->$id.name****taskId=$mapId")
              // scalastyle:on  println
              blockManager.riffleReadSuccess(id)
            }
            blockManager.release
            mergeBlocksLengths = riffleBlocks.length
            return (true, riffleBlocks)
          } else {
            blockManager.release
            return (false, null.asInstanceOf[Seq[ShuffleBlockId]])
          }
        }
      }
    }
    // It will never arrive this code
    (false, null.asInstanceOf[Seq[ShuffleBlockId]])
  }

  def getRiffleInfo(ids: Seq[ShuffleBlockId]) :
  Map[ShuffleBlockId, (Long, Boolean, Array[Long])] = {
    for (i <- ids) {
      val index = shuffleBlockResolver.getSegmentIndex(i)
      blockIdRead += (i -> ((0L, false, index)))
      // scalastyle:off  println
      println(s"blockIndex_Print****taskId=$mapId")
      // scalastyle:on  println
      for (mm <- index) {
        print(mm + " ")
      }
    }
    blockIdRead
  }
  def readBlock() : Unit = {
    for (id <- blockIdRead.keys) {
      val info = blockIdRead(id)
      // check if it has been read EOF
      if (info._2) {
        // Do not read this block
        // release byte buffer
        blockIdRead -= id
        readResult -= id
      } else {
        // read this block
        val read = if (info._1 + readSize <= info._3.last) readSize else (info._3.last - info._1).intValue()
        val offset = info._1
        val buf = blockManager.getRiffleBlockData(id, offset, read)
        val inputStream = buf.createInputStream()
        var flag = false
        if (!readResult.contains(id)) {
          val byte = new Array[Byte](read)
          inputStream.read(byte)
          readResult += (id -> byte)
          if (read < readSize) {
            flag = true
          }
        } else {
          if (read < readSize || info._1 + read == info._3.last) {
            val byte = new Array[Byte](read)
            inputStream.read(byte)
            readResult += (id -> byte)
            flag = true
          } else {
            val byte = readResult(id)
            inputStream.read(byte)
            readResult += (id -> byte)
          }
        }
        blockIdRead += (id -> ((info._1 + readSize, flag, info._3)))
      }
    }
  }

  def mergeRiffleBlocks() : Unit = {
    for ((id, info) <- blockIdRead) {
      val result = readResult(id)
      val index = info._3
      val start = info._1 - readSize
      var startSegmentId = Int.MaxValue
      var startSegmentFlag = false
      val end = Math.min(info._1, info._3.last)
      var endSegmentId = -1
      var endSegmentFlag = false
      var max = -1L
      for (i <- index.indices) {
        if (i < index.length-1 && start < index(i + 1) && start >= index(i)) {
          startSegmentId = i
          if (start == index(i)) {
            startSegmentFlag = true
          }
          // set startSegmentId=0 at the first time
          if (start == 0L) {
            startSegmentId = 0
          }
        }
        if (i < index.length-1 && end <= index(i + 1) && end > index(i)  ) {
          endSegmentId = i
          max = index(i + 1)
          if (end == index(i + 1)) {
            endSegmentFlag = true
          }
        }
        if (max == index(i)) {
          endSegmentId = i - 1
        }
      }
      for (segment <- startSegmentId  to  endSegmentId ) {
        if (segment == startSegmentId && !segmentStatuses.contains((id, segment))) {
          val segmentByte = result.slice(0, (index(segment + 1) - start).intValue)
          segmentStatuses.put((id, segment), (startSegmentFlag, segmentByte))
        } else if (segment == startSegmentId && segmentStatuses.contains((id, segment))) {
          val segmentByte = result.slice(0, (index(segment + 1) - start).intValue)
          val lastReadByte = segmentStatuses.get((id, segment)).get._2
          val newByte = lastReadByte ++ segmentByte
          if (newByte.length == index(segment + 1) - index(segment)) {
            segmentStatuses.update((id, segment), (true, newByte))
          } else {
            print("\nMay error\n")
            print("\nstart = " + start + " segment = " + segment +" index(segment) = "
              + index(segment) + " index(segment+1) = " + index(segment + 1) + "\n")
            print("oldByte = " + lastReadByte.length + " readByte = "
              + segmentByte.length + " newByte = " + newByte.length)
            segmentStatuses.update((id, segment), (false, newByte))
          }
        }
        // we assumed endSegmentId > startSegmentId
        if (segment == endSegmentId) {
          val segmentByte = result.slice((index(segment) - start).intValue, (end - start).intValue)
          segmentStatuses.put((id, segment), (endSegmentFlag, segmentByte))
        }
        if (segment > startSegmentId && segment < endSegmentId) {
          if (id.name.equals("shuffle_0_0_0")) {
            print("startSegment------------->" + startSegmentId)
            print("segment------------->" + segment)
            print("endSegmentId------------->" + endSegmentId)
          }
          val segmentByte = result.slice((index(segment) - start).intValue,
            (index(segment + 1) - start).intValue())
          segmentStatuses.put((id, segment), (true, segmentByte))
        }
      }
    }
    for (((id, segment), (flag, byte)) <- segmentStatuses) {
        print("\nshuffleId = " + id.name + " segment = " + segment +
          " flag = " + flag +  " byteLength " + byte.length)
    }
  }
  def writeToDisk(writer: DataOutputStream, shuffleBlockIds: Seq[ShuffleBlockId]) : Unit = {
    try {
      val writeInfo = new Array[Int](partitionLengths.length)
      for (((_, segment), (flag, _)) <- segmentStatuses) {
        if (flag) writeInfo(segment) += 1
      }
      print("----------------write info---------------\n")
      for (i <- writeInfo.indices) {
        print("segment = " + i + " times " + writeInfo(i) + "\n")
      }
      print("----------------write info---------------\n")
      for (i <- writeInfo.indices) {
        if (writeInfo(i) == mergeBlocksLengths) {
          for (id <- shuffleBlockIds) {
            if (segmentStatuses.contains((id, i))) {
              val value = segmentStatuses((id, i))._2
              if (value.length != 0) {
                writer.write(value, 0, value.length)
                print("\n****write**** id = " +id.name +
                  " segment = " + i + " length = " + value.length)
              }
              segmentStatuses.remove((id, i))
              rifflePartitionLengths(i) += value.length
            }
          }
        }
//        val segment = writer.commitAndGet()
        writer.flush()
      }
    } catch {
      case e: Exception =>
        logError("Write to Disk error")
        e.printStackTrace()
    }
  }




  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        blockManager.insertTaskResultInfo(
          ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID))
        return Option(mapStatus)
      } else {
        return None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        writeMetrics.incWriteTime(System.nanoTime - startTime)
        sorter = null
      }
    }
  }
}

private[spark] object SortShuffleWriter {
  def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
    // We cannot bypass sorting if we need to do map-side aggregation.
    if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      false
    } else {
      val bypassMergeThreshold: Int = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
      dep.partitioner.numPartitions <= bypassMergeThreshold
    }
  }
}
