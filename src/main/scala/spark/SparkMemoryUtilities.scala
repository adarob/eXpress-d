package spark

import org.apache.spark.SparkEnv
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.SizeEstimator

object SparkMemoryUtilities {

  /**
   * Helper for dropping broadcast variables directly from the BlockManager created for each
   * local Spark process.
   */
  def dropBroadcastVar(bcVar: Broadcast[_]) {
    bcVar.unpersist()

    //val blockManager = SparkEnv.get.blockManager
    //val broadcastBlockId = bcVar.asInstanceOf[HttpBroadcast[_]].blockId
    //blockManager.removeBlock(broadcastBlockId)
  }

  def estimateSize(obj: AnyRef): Long = {
    return SizeEstimator.estimate(obj)
  }
}
