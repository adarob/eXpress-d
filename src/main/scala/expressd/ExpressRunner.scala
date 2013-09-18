package expressd

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.Traversable

import joptsimple.{OptionSet, OptionParser}

import spark.{Accumulable, AccumulableParam}
import spark.broadcast.{HttpBroadcast, Broadcast}
import spark.{RDD, SparkContext, SparkEnv}
import spark.storage._

import spark.SparkContext
import spark.SparkContext._

import spark.SparkMemoryUtilities

object ExpressRunner {

  val HITS_FILE_PATH =    ("hits-file-path", "path to the hits.pb file")
  val TARGETS_FILE_PATH = ("targets-file-path", "path to the targets.pb file")
  val SHOULD_USE_BIAS =   ("should-use-bias", "whether to incorporate bias modeling")
  val NUM_ITERATIONS =    ("num-iterations", "max number of iterations to run")
  val SHOULD_CACHE =      ("should-cache", "if true, cache the fragments RDD")
  val NUM_PARTITIONS_FOR_ALIGNMENTS =
    ("num-partitions-for-alignments", "number of partitions for the RDD containing fragments")
  val DEBUG_OUTPUT =      ("debug-output", "if true, print out chatty debug output")

  val intOptions = Seq(NUM_ITERATIONS, NUM_PARTITIONS_FOR_ALIGNMENTS)
  val stringOptions = Seq(HITS_FILE_PATH, TARGETS_FILE_PATH)
  val booleanOptions = Seq(SHOULD_USE_BIAS, SHOULD_CACHE, DEBUG_OUTPUT)
  val options = intOptions ++ stringOptions ++ booleanOptions

  val parser = new OptionParser()
  var optionSet: OptionSet = _


  def main(args: Array[String]) {

    val optionSet = parser.parse(args.toSeq: _*)

    val hitsFilePath = optionSet.valueOf(HITS_FILE_PATH._1).asInstanceOf[String]
    val targetsFilePath = optionSet.valueOf(TARGETS_FILE_PATH._1).asInstanceOf[String]
    var shouldUseBias = optionSet.valueOf(SHOULD_USE_BIAS._1).asInstanceOf[Boolean]
    var numIterations = optionSet.valueOf(NUM_ITERATIONS._1).asInstanceOf[Int]
    val shouldCache = optionSet.valueOf(SHOULD_CACHE._1).asInstanceOf[Boolean]
    var numPartitions = optionSet.valueOf(NUM_PARTITIONS_FOR_ALIGNMENTS._1).asInstanceOf[Int]

    // Check that there are enough command-line arguments provided. Express-D only requires that a
    // master is passed.
    if (args.length < 2) {
      println("Express-D requires a 'master' and an option set.")
      System.exit(1)
    }
    val host = args(0)

    // Start processing
    val isLocal = host.toLowerCase.equals("local")

    val sc = new SparkContext(host, "express-" + host)

    // Initialize the environment so that all constants/static variables will be initialized
    // (Scala's static variabels are lazily initialized).
    ExpressEnv.initWithSparkContext(sc)

    println("started preprocessing")

    val preProcessingTime = System.currentTimeMillis()

    var fragmentsRDD: RDD[ExpressD.Fragment] = sc.textFile(hitsFilePath).map { line =>
        ExpressD.createFragment(line)
      }

    // Coalesce into N partitions, if arg is given
    if (numPartitions != -1) {
      fragmentsRDD = fragmentsRDD.coalesce(numPartitions)
    }
    
    var preprocessedTargetsRDD = sc.textFile(targetsFilePath).cache
    preprocessedTargetsRDD.count

    var preprocessedTargetsArray = preprocessedTargetsRDD.collect
 
    var targetsArray = preprocessedTargetsArray.map { line =>
      Deserializer.deserializeTarget(line)
    }

    val targetsBuffer = new ArrayBuffer[ExpressD.Target]()
    for (targetMsg <- targetsArray) {
      targetsBuffer.append(
        ExpressD.Target(
          targetMsg.getName(),
          targetMsg.getId(),
          targetMsg.getLength(),
          targetMsg.getSeq().toByteArray()))
    }

    var sortedTargets = targetsBuffer.toArray.sortWith((x, y) => x.id < y.id)


    // Preprocessing
    var bcTargSeqs: Broadcast[Array[Array[Byte]]] = sc.broadcast(sortedTargets.map(target => target.seq))
    var bcTargLens: Broadcast[Array[Int]] = sc.broadcast(sortedTargets.map(target => target.length))
    if (shouldCache) {
      println("Caching bias and error indices...")
      fragmentsRDD = ExpressD.cacheErrorsAndBias(fragmentsRDD, bcTargSeqs, bcTargLens)
    }

    val shouldPersistSer = ExpressEnv.SHOULD_SERIALIZE_FRAGMENTS

    if (shouldPersistSer) {
      fragmentsRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
      println("persisting serialized")
    } else {
      fragmentsRDD.persist(StorageLevel.MEMORY_AND_DISK)
      println("persisting deserialized")
    }

    fragmentsRDD.foreach(_ => Unit)
    println("preprocessing finished! time: " + (System.currentTimeMillis() - preProcessingTime))
    // TODO: make a perf class
    var runExpressStartTime = System.currentTimeMillis()

    // Take a count.
    val totalNumFragments = fragmentsRDD.count
    // Broken up for debugging

    println("num of targets: " + sortedTargets.size)
    println("size of targets: " + SparkMemoryUtilities.estimateSize(sortedTargets))

    val parseTime = System.currentTimeMillis()

    val outputRDD = ExpressD.runExpress(bcTargSeqs,
                                        bcTargLens,
                                        sortedTargets,
                                        fragmentsRDD,
                                        numIterations,
                                        shouldUseBias,
                                        isLocal)

    val iterTime = System.currentTimeMillis()

    // Finished!
    val outputDivider = "=" * 50
    println(outputDivider)
    val finishTime = System.currentTimeMillis()
    println("Took " + (parseTime - runExpressStartTime) + " ms to load and parse " + totalNumFragments +
      " fragments from disk")
    println("Took " + (iterTime - parseTime) + " ms for " + numIterations + " iteration" +
      (if (numIterations == 1) "" else "s"))
    println("Took " + (finishTime - iterTime) + " ms to write to disk")
    println("Took " + (finishTime - runExpressStartTime) + " ms total time")
    println(outputDivider)

    System.exit(0)
  }
}
