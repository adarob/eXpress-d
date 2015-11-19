package spark

import org.apache.spark.SparkEnv
import org.apache.spark.broadcast.{HttpBroadcast, Broadcast}

object SparkMemoryUtilities {

  /**
   * Helper for dropping broadcast variables directly from the BlockManager created for each
   * local Spark process.
   */
  def dropBroadcastVar(bcVar: Broadcast[_]) {
    val blockManager = SparkEnv.get.blockManager
    val broadcastBlockId = bcVar.asInstanceOf[HttpBroadcast[_]].blockId
    blockManager.removeBlock(broadcastBlockId)
  }

  def estimateSize(obj: AnyRef): Long = {
    return SizeEstimator.estimate(obj)
  }
}
