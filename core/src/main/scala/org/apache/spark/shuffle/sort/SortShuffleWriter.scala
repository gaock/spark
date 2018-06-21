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

  private var mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker
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
    logInfo(s"isUseRiffle:$isUseRiffle **** test is use riffle ***taskId=$mapId")
    if (isUseRiffle) {
      logInfo(s"We Use Riffle to Merge Files!****taskId=$mapId")
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
        //        val blockId = ShuffleBlockId(dep.shuffleId, mapId, 100)
        val blockId = ShuffleBlockId(dep.shuffleId, mapId, 100)
        val writer = blockManager.getDiskWriter(
          blockId, tmp, dep.serializer.newInstance(), fileBufferSize, new ShuffleWriteMetrics)
        while (blockIdRead.nonEmpty) {
          readBlock()
          val segments = mergeRiffle()
          writeToDisk(segments._1 - 1, segments._2 - 1, writer, readSegmentInfo)
          copyToByteArray(segments._2, segments._3, readSegmentInfo)
        }
        shuffleBlockResolver.writeRiffleIndexFileAndCommit(dep.shuffleId, mapId,
          rifflePartitionLengths, tmp)
        mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths,
          rifflePartitionLengths, res._2.toArray)
      } else {
        logInfo(s"waiting threshold files***taskId=$mapId")
      }
    }
  }

  def isRiffleMerge(): (Boolean, Seq[ShuffleBlockId]) = {
//    mapOutputTracker.synchronized {
//      val status = mapOutputTracker.getMapStatuses()
//      if (status.contains(dep.shuffleId)) {
//        val mapStatus = status(dep.shuffleId)
//        val length = mapStatus.length
//        val shuffleId = dep.shuffleId
//        logInfo(s"mapStatus length $length*****shuffle-$shuffleId*****task-$mapId")
//        if (length < riffleThreshold) {
//          for (m <- mapStatus) {
//            m.
//          }
//          return (true, for ())
//        }
//      }
//    }

    blockManager.synchronized{
      var success = false
      val b1 = blockManager.getMatchingBlockIds(block => true)
      val b1length = b1.length
      for (i <- b1) {
        val name = i.name
        logInfo(s"blockId name---->$name && taskId=$mapId && total num = $b1length")
      }
      val shuffleBlockIds = b1.filter(blockId => {(
        blockId.isShuffleIndex &&
        blockId.asInstanceOf[ShuffleIndexBlockId].shuffleId == dep.shuffleId &&
        !blockId.asInstanceOf[ShuffleIndexBlockId].flag)})
      val shuffleLength = shuffleBlockIds.length
      for (i <- shuffleBlockIds) {
        val name = i.name
        logInfo(s"*****blockId name---->$name && taskId=$mapId && total num = $shuffleLength")
      }
      // It may reduce efficient since this code will search all blocks
      if (shuffleBlockIds.length > riffleThreshold) {
        logInfo(s"start merge riffle blocks, " +
          s"the blocks num has been over the riffle threshold!!!taskId=$mapId")
        // Change blocks' flag in case other tasks change it.
        for (id <- shuffleBlockIds) {
          id.asInstanceOf[ShuffleIndexBlockId].change(true)
        }
        success = true
        val riffleBlocks = new Array[ShuffleBlockId](shuffleBlockIds.length)
        val it = shuffleBlockIds.iterator
        for (i <- shuffleBlockIds.indices) {
          val mapId = it.next().asInstanceOf[ShuffleIndexBlockId].getMapId
          riffleBlocks(i) = new ShuffleBlockId(dep.shuffleId, mapId, 0)
        }
        return (success, riffleBlocks.toSeq)
      } else {
        logInfo(s"waiting enough block && " +
          s"release the blockManager lock && " +
          s"the block total num = $b1" +
          s"the shuffle block num = $shuffleLength")
        return (success, null.asInstanceOf[Seq[ShuffleBlockId]])
      }
    }
  }

  def getRiffleInfo(ids: Seq[ShuffleBlockId]) :
  Map[ShuffleBlockId, (Long, Boolean, Array[Long])] = {
    for (i <- ids) {
      val index = shuffleBlockResolver.getSegmentIndex(i)
      blockIdRead += (i -> ((0L, false, index)))
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
          if (read < readSize) {
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

  def mergeRiffle() : (Int, Int, Int) = {
    // Write the min-maxMin segments to Disk.
    // Write the maxMin-max segments to memory. It's stored as Map[Segment, Array[Byte]]
    var min = Int.MaxValue
    var maxMin = -1
    var max = -1
    for (id <- blockIdRead.keys) {
      val info = blockIdRead(id)._3
      val start = blockIdRead(id)._1 - readSize
      val end = blockIdRead(id)._1
      var inMin = Int.MaxValue
      var inMax = -1
      for (i <- info.indices if inMin == Int.MaxValue) {
        if (start < info(i)) inMin = i - 1
        if (start == info(i)) inMin = i
      }
      for (i <- info.indices if inMax == -1) {
        if (end <= info(i)) inMax = i - 1
        if (end > info.last) inMax = info.length - 2
      }
      readSegmentInfo += (id -> ((inMin, inMax)))
      max = if (max > inMax) max else inMax
      min = if (min < inMin) min else inMin
      maxMin = if (inMax > maxMin) maxMin else inMax
    }
    return (min, maxMin, max)
  }
  def writeToDisk(min: Int, max: Int, writer: DiskBlockObjectWriter,
                  readSegmentInfo: Map[ShuffleBlockId, (Int, Int)]): Unit = {
    try {
      for (segment <- min to max) {
        if (segment >= 0) {
          for (id <- blockIdRead.keys) {
            if (memoryByte.contains((segment, id))) {
              // write this block segment(in memory store) to disk
              writer.write(memoryByte((segment, id)))
              memoryByte -= ((segment, id))
            }
            // write this block segment(read last time) to disk
            val info = findOffAndLen(id, segment, readSegmentInfo)
            if (info._1 != -1) {
              writer.write(readResult(id), info._1, info._2)
            }
          }
        }
        // record the index of segment
        val riffleSegment = writer.commitAndGet()
        rifflePartitionLengths(segment) = riffleSegment.length
      }
    } catch {
      case e: Exception =>
        logError("Write to Disk error")
        e.printStackTrace()
    }
  }

  // It should be Map[segment, Map[blockId, Array[Byte]]

  def copyToByteArray(min: Int, max : Int,
                      readSegmentInfo: Map[ShuffleBlockId, (Int, Int)]) : Unit = {
    for (segment <- min to  max) {
      for (id <- blockIdRead.keys) {
        val info = findOffAndLen(id, segment, readSegmentInfo)
        if (info._1 != -1) {
          val tmp = new Array[Byte](info._2)
          for (i <- tmp.indices) {
            tmp(i) = readResult(id)(i + info._1)
          }
          memoryByte += ((segment, id) -> tmp)
        }
      }
    }
  }
  def findOffAndLen(id : ShuffleBlockId, segment: Int, readStoreInfo :
  Map[ShuffleBlockId, (Int, Int)]): (Int, Int) = {
    if (segment == readStoreInfo(id)._1) {
      val location = 0
      val length = blockIdRead(id)._3(segment + 1) - (blockIdRead(id)._1 - readSize)
      return (location.intValue(), length.intValue())
    } else if (segment ==  readStoreInfo(id)._2) {
      val location = blockIdRead(id)._3(segment) - (blockIdRead(id)._1 - readSize)
      val length = if (blockIdRead(id)._1 <= blockIdRead(id)._3.last) {
        blockIdRead(id)._1 - blockIdRead(id)._3(segment)}
      else blockIdRead(id)._3.last - blockIdRead(id)._3(segment)
      return (location.intValue(), length.intValue())
    } else if (segment < readStoreInfo(id)._2 && segment > readStoreInfo(id)._1) {
      val location = blockIdRead(id)._3(segment) - (blockIdRead(id)._1 - readSize)
      val length = blockIdRead(id)._3(segment + 1) - blockIdRead(id)._3(segment)
      return (location.intValue(), length.intValue())
    }
    (-1, -1)
  }


  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
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
