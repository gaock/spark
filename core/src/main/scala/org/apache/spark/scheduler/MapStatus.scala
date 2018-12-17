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

package org.apache.spark.scheduler

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.SparkEnv
import org.apache.spark.internal.config
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.Utils

/**
 * Result returned by a ShuffleMapTask to a scheduler. Includes the block manager address that the
 * task ran on as well as the sizes of outputs for each reducer, for passing on to the reduce tasks.
 */
private[spark] sealed trait MapStatus {
  /** Location where this task was run. */
  def location: BlockManagerId

  def getSizeForRiffleBlock(reduceId: Int): Long
  /**
   * Estimated size for the reduce block, in bytes.
   *
   * If a block is non-empty, then this method MUST return a non-zero size.  This invariant is
   * necessary for correctness, since block fetchers are allowed to skip zero-size blocks.
   */
  def getSizeForBlock(reduceId: Int): Long

  def getShuffleBlockIds: Array[ShuffleBlockId]

  def toRiffleMapStatus(id: ShuffleBlockId) : Unit

  def toMapStatus : Unit

}


private[spark] object MapStatus {

  def apply(loc: BlockManagerId, uncompressedSizes: Array[Long]): MapStatus = {
    if (uncompressedSizes.length > 2000) {
      HighlyCompressedMapStatus(loc, uncompressedSizes)
    } else {
      new CompressedMapStatus(loc, uncompressedSizes)
    }
  }
  def apply(loc: BlockManagerId, uncompressedSizes: Array[Long],
            riffleUncompressedSizes: Array[Long], ids: Array[ShuffleBlockId]) : MapStatus = {
      new CompressedMapStatus(loc, uncompressedSizes, riffleUncompressedSizes, ids)
  }

  private[this] val LOG_BASE = 1.1

  /**
   * Compress a size in bytes to 8 bits for efficient reporting of map output sizes.
   * We do this by encoding the log base 1.1 of the size as an integer, which can support
   * sizes up to 35 GB with at most 10% error.
   */
  def compressSize(size: Long): Byte = {
    if (size == 0) {
      0
    } else if (size <= 1L) {
      1
    } else {
      math.min(255, math.ceil(math.log(size) / math.log(LOG_BASE)).toInt).toByte
    }
  }

  /**
   * Decompress an 8-bit encoded block size, using the reverse operation of compressSize.
   */
  def decompressSize(compressedSize: Byte): Long = {
    if (compressedSize == 0) {
      0
    } else {
      math.pow(LOG_BASE, compressedSize & 0xFF).toLong
    }
  }
}


/**
 * A [[MapStatus]] implementation that tracks the size of each block. Size for each block is
 * represented using a single byte.
 *
 * @param loc location where the task is being executed.
 * @param compressedSizes size of the blocks, indexed by reduce partition id.
 */
private[spark] class CompressedMapStatus(
    private[this] var loc: BlockManagerId,
    private[this] var compressedSizes: Array[Byte],
    private[this] var riffleCompressedSizes: Array[Byte] = null,
    private[this] var riffleBlockIds: Array[ShuffleBlockId] = null)
  extends MapStatus with Externalizable {

  protected def this() = this(null, null.asInstanceOf[Array[Byte]])

  // For deserialization only

  def this(loc: BlockManagerId, uncompressedSizes: Array[Long]) {
    this(loc, uncompressedSizes.map(MapStatus.compressSize))
  }

  def this(loc: BlockManagerId, uncompressedSizes: Array[Long],
           riffleCompressedSizes: Array[Long], riffleBlockIds: Array[ShuffleBlockId]) {
    this(loc, uncompressedSizes.map(MapStatus.compressSize),
      riffleCompressedSizes.map(MapStatus.compressSize), riffleBlockIds)
  }

  override def location: BlockManagerId = loc

  override def getSizeForBlock(reduceId: Int): Long = {
    MapStatus.decompressSize(compressedSizes(reduceId))
  }

  override def getSizeForRiffleBlock(reduceId: Int): Long = {
    if (riffleCompressedSizes == null.asInstanceOf[Array[Long]]) {
      return 0
    }
    MapStatus.decompressSize(riffleCompressedSizes(reduceId))
  }

  override def getShuffleBlockIds: Array[ShuffleBlockId] = {
    riffleBlockIds
  }

  override def toRiffleMapStatus(id: ShuffleBlockId): Unit = {
    this.loc = loc
    this.compressedSizes = compressedSizes
    this.riffleCompressedSizes = compressedSizes
    this.riffleBlockIds = Array(id)

  }

  override def toMapStatus: Unit = {
    this.loc = loc
    this.compressedSizes = compressedSizes
    this.riffleCompressedSizes = null
    this.riffleBlockIds = null
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    out.writeInt(compressedSizes.length)
    out.write(compressedSizes)
    if (riffleCompressedSizes != null.asInstanceOf[Array[Byte]]) {
      out.writeInt(riffleCompressedSizes.length)
      out.write(riffleCompressedSizes)
      out.writeInt(riffleBlockIds.length)
      out.writeObject(riffleBlockIds)
    } else out.writeInt(-1)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    val len = in.readInt()
    compressedSizes = new Array[Byte](len)
    in.readFully(compressedSizes)
    val leng = in.readInt()
    if (leng != -1) {
      riffleCompressedSizes = new Array[Byte](leng)
      in.readFully(riffleCompressedSizes)
      in.readInt()
      riffleBlockIds = in.readObject().asInstanceOf[Array[ShuffleBlockId]]
    } else {
      riffleCompressedSizes = null.asInstanceOf[Array[Byte]]
      riffleBlockIds = null.asInstanceOf[Array[ShuffleBlockId]]
    }
  }
}

/**
 * A [[MapStatus]] implementation that stores the accurate size of huge blocks, which are larger
 * than spark.shuffle.accurateBlockThreshold. It stores the average size of other non-empty blocks,
 * plus a bitmap for tracking which blocks are empty.
 *
 * @param loc location where the task is being executed
 * @param numNonEmptyBlocks the number of non-empty blocks
 * @param emptyBlocks a bitmap tracking which blocks are empty
 * @param avgSize average size of the non-empty and non-huge blocks
 * @param hugeBlockSizes sizes of huge blocks by their reduceId.
 */
private[spark] class HighlyCompressedMapStatus private (
    private[this] var loc: BlockManagerId,
    private[this] var numNonEmptyBlocks: Int,
    private[this] var emptyBlocks: RoaringBitmap,
    private[this] var avgSize: Long,
    private var hugeBlockSizes: Map[Int, Byte],
    private[this] var numNonEmptyBlocks2: Int = -1,
    private[this] var emptyBlocks2: RoaringBitmap = null,
    private[this] var avgSize2: Long = -1,
    private var hugeBlockSizes2: Map[Int, Byte] = null,
    private var ids: Array[ShuffleBlockId] = null)
  extends MapStatus with Externalizable {

  // loc could be null when the default constructor is called during deserialization
  require(loc == null || avgSize > 0 || hugeBlockSizes.size > 0 || numNonEmptyBlocks == 0,
    "Average size can only be zero for map stages that produced no output")

  protected def this() = this(null, -1, null, -1, null)  // For deserialization only
  override def location: BlockManagerId = loc

  override def getSizeForBlock(reduceId: Int): Long = {
    assert(hugeBlockSizes != null)
    if (emptyBlocks.contains(reduceId)) {
      0
    } else {
      hugeBlockSizes.get(reduceId) match {
        case Some(size) => MapStatus.decompressSize(size)
        case None => avgSize
      }
    }
  }

  override def getSizeForRiffleBlock(reduceId: Int): Long = {
    if (hugeBlockSizes2 == null) {
      return 0
    }
    if (emptyBlocks2.contains(reduceId)) {
      0
    } else {
      hugeBlockSizes2.get(reduceId) match {
        case Some(size) => MapStatus.decompressSize(size)
        case None => avgSize2
      }
    }
  }

  override def getShuffleBlockIds(): Array[ShuffleBlockId] = {
    this.ids
  }
  // We'll never use these functions.
  override def toRiffleMapStatus(id: ShuffleBlockId): Unit = {}

  override def toMapStatus: Unit = {}

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    emptyBlocks.writeExternal(out)
    out.writeLong(avgSize)
    out.writeInt(hugeBlockSizes.size)
    hugeBlockSizes.foreach { kv =>
      out.writeInt(kv._1)
      out.writeByte(kv._2)
    }
    if (hugeBlockSizes2 != null) {
      out.writeBoolean(true)
      emptyBlocks2.writeExternal(out)
      out.writeLong(avgSize2)
      out.writeInt(hugeBlockSizes2.size)
      hugeBlockSizes2.foreach { kv =>
        out.writeInt(kv._1)
        out.writeByte(kv._2)
      }
      out.writeObject(ids)
    } else {
      out.writeBoolean(false)
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    emptyBlocks = new RoaringBitmap()
    emptyBlocks.readExternal(in)
    avgSize = in.readLong()
    val count = in.readInt()
    val hugeBlockSizesArray = mutable.ArrayBuffer[Tuple2[Int, Byte]]()
    (0 until count).foreach { _ =>
      val block = in.readInt()
      val size = in.readByte()
      hugeBlockSizesArray += Tuple2(block, size)
    }
    hugeBlockSizes = hugeBlockSizesArray.toMap

    if (in.readBoolean()) {
      emptyBlocks2 = new RoaringBitmap()
      emptyBlocks2.readExternal(in)
      avgSize2 = in.readLong()
      val count2 = in.readInt()
      val hugeBlockSizesArray2 = mutable.ArrayBuffer[Tuple2[Int, Byte]]()
      (0 until count2).foreach { _ =>
        val block = in.readInt()
        val size = in.readByte()
        hugeBlockSizesArray2 += Tuple2(block, size)
        }
        hugeBlockSizes2 = hugeBlockSizesArray2.toMap
        ids = in.readObject().asInstanceOf[Array[ShuffleBlockId]]
      }
    else {
      numNonEmptyBlocks2 = -1
      emptyBlocks2 = null
      avgSize2 = -1
      hugeBlockSizes2 = null
      ids = null
    }
  }
}

private[spark] object HighlyCompressedMapStatus {
  def apply(loc: BlockManagerId, uncompressedSizes: Array[Long],
            riffleUncompressedSizes: Array[Long] = null, ids: Array[ShuffleBlockId] = null)
  : HighlyCompressedMapStatus = {
    // We must keep track of which blocks are empty so that we don't report a zero-sized
    // block as being non-empty (or vice-versa) when using the average block size.
    var i = 0
    var numNonEmptyBlocks: Int = 0
    var numSmallBlocks: Int = 0
    var totalSmallBlockSize: Long = 0
    // From a compression standpoint, it shouldn't matter whether we track empty or non-empty
    // blocks. From a performance standpoint, we benefit from tracking empty blocks because
    // we expect that there will be far fewer of them, so we will perform fewer bitmap insertions.
    val emptyBlocks = new RoaringBitmap()
    val totalNumBlocks = uncompressedSizes.length
    val threshold = Option(SparkEnv.get)
      .map(_.conf.get(config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD))
      .getOrElse(config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD.defaultValue.get)
    val hugeBlockSizesArray = ArrayBuffer[Tuple2[Int, Byte]]()
    while (i < totalNumBlocks) {
      val size = uncompressedSizes(i)
      if (size > 0) {
        numNonEmptyBlocks += 1
        // Huge blocks are not included in the calculation for average size, thus size for smaller
        // blocks is more accurate.
        if (size < threshold) {
          totalSmallBlockSize += size
          numSmallBlocks += 1
        } else {
          hugeBlockSizesArray += Tuple2(i, MapStatus.compressSize(uncompressedSizes(i)))
        }
      } else {
        emptyBlocks.add(i)
      }
      i += 1
    }
    val avgSize = if (numSmallBlocks > 0) {
      totalSmallBlockSize / numSmallBlocks
    } else {
      0
    }
    emptyBlocks.trim()
    emptyBlocks.runOptimize()
    if (riffleUncompressedSizes != null) {
      val info = getRiffleInfo(loc, riffleUncompressedSizes)
      return new HighlyCompressedMapStatus(loc, numNonEmptyBlocks, emptyBlocks, avgSize,
        hugeBlockSizesArray.toMap, info._2, info._3, info._4, info._5, ids)
    }
    new HighlyCompressedMapStatus(loc, numNonEmptyBlocks, emptyBlocks, avgSize,
      hugeBlockSizesArray.toMap)
  }
  def getRiffleInfo (loc: BlockManagerId, uncompressedSizes: Array[Long]): (
  BlockManagerId, Int, RoaringBitmap, Long, Map[Int, Byte]) = {
    // We must keep track of which blocks are empty so that we don't report a zero-sized
    // block as being non-empty (or vice-versa) when using the average block size.
    var i = 0
    var numNonEmptyBlocks: Int = 0
    var numSmallBlocks: Int = 0
    var totalSmallBlockSize: Long = 0
    // From a compression standpoint, it shouldn't matter whether we track empty or non-empty
    // blocks. From a performance standpoint, we benefit from tracking empty blocks because
    // we expect that there will be far fewer of them, so we will perform fewer bitmap insertions.
    val emptyBlocks = new RoaringBitmap()
    val totalNumBlocks = uncompressedSizes.length
    val threshold = Option(SparkEnv.get)
      .map(_.conf.get(config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD))
      .getOrElse(config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD.defaultValue.get)
    val hugeBlockSizesArray = ArrayBuffer[Tuple2[Int, Byte]]()
    while (i < totalNumBlocks) {
      val size = uncompressedSizes(i)
      if (size > 0) {
        numNonEmptyBlocks += 1
        // Huge blocks are not included in the calculation for average size, thus size for smaller
        // blocks is more accurate.
        if (size < threshold) {
          totalSmallBlockSize += size
          numSmallBlocks += 1
        } else {
          hugeBlockSizesArray += Tuple2(i, MapStatus.compressSize(uncompressedSizes(i)))
        }
      } else {
        emptyBlocks.add(i)
      }
      i += 1
    }
    val avgSize = if (numSmallBlocks > 0) {
      totalSmallBlockSize / numSmallBlocks
    } else {
      0
    }
    emptyBlocks.trim()
    emptyBlocks.runOptimize()
    return (loc, numNonEmptyBlocks, emptyBlocks, avgSize,
      hugeBlockSizesArray.toMap)
  }
}
