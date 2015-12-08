package expressd

import java.io.{BufferedReader, BufferedWriter}
import java.io.{File, FileInputStream, FileOutputStream, FileWriter}
import java.io.{InputStreamReader, OutputStreamWriter}
import java.io.IOException
import java.util.LinkedList
import java.util.Random

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.Traversable

import it.unimi.dsi.fastutil.bytes.ByteArrayList
import it.unimi.dsi.fastutil.doubles.DoubleArrayList
import it.unimi.dsi.fastutil.objects.ObjectArrayList

import expressd.protobuf._
import spark.SparkMemoryUtilities

import spark.{Accumulable, AccumulableParam}
import spark.broadcast.{HttpBroadcast, Broadcast}
import spark.{RDD, SparkContext, SparkEnv}
import spark.storage._

import spark.SparkContext
import spark.SparkContext._

// For debugging.
import scala.runtime.ScalaRunTime._

import ExpressEnv._

object ExpressD {
  // TODO: make these configs/system properties
  val BIAS_MATRIX_SIZE = BIAS_WINDOW * 2 + 1
  val BYTE_OFFSET = 128
  val BIAS_ORDER = 3
  val ERROR_ORDER = 2


  var runExpressStartTime = 0L

  // Used for convergence
  val LOG_0 = math.log(0)
  val LOG_1 = math.log(1)
  val INF = 1.0/0.0

  //set seed for identical results between runs, e.g. Random(42)
  var rand = new Random()

  // Starts, Lengths, mismatchIndices, mismatchNucs
  type ReadAlignments = (
    Array[Int],         // ._1 == start/end positions
    Array[Byte],        // ._2 == lengths
    Array[Array[Byte]], // ._3 == mismatchIndices OR errorIndices
    Array[Array[Byte]], // ._4 == mismatchNucs OR biasIndices
    Array[Byte]         // ._5 == biasStartPos OR -1
  )

  // Stores the attributes (target Ids, leftFirsts, Left ReadAlignments, Right ReadAlignments)
  // of the alignments of a single Fragment in parallel arrays (for memory savings).
  type Fragment = (
    Array[Int],        // ._1 == targetIds
    Array[Boolean],    // ._2 == leftFirsts
    ReadAlignments,    // ._3 == readLs
    ReadAlignments     // ._4 == readRs
  )

  // Implicit arg passed when initializaing a SparkContext.Accumulable[Array[Double], (Int, Double)].
  implicit object ArrayAccum extends AccumulableParam[Array[Double], (Int, Double)] {

    def addInPlace(t1: Array[Double], t2: Array[Double]): Array[Double] = {
      var i = 0
      while (i < t1.size) {
        t1(i) += t2(i)
        i += 1
      }
      return t1
    }
    // This isn't used, to minimize tmp Tuple creation.
    def addAccumulator(t1: Array[Double], t2: (Int, Double)): Array[Double] = {
      t1(t2._1) += t2._2
      t1
    }

    def zero(t: Array[Double]): Array[Double] = {
      return new Array[Double](t.size)
    }
  }

  // Implicit arg passed when initializaing a SparkContext.Accumulable[MarkovChain, (Int, Byte, Double)].
  // Should we actually treat the MarkovChains as an array of accumulable Arrays instead??
  // When MarkovChain becomes a class, use new accessor and mutator instead of decodeMarkovChainIndex method.
  implicit object MarkovChainAccum
      extends AccumulableParam[Array[Array[Array[Double]]], (Int, Byte, Double)] {

    def addInPlace(t1: Array[Array[Array[Double]]], t2: Array[Array[Array[Double]]])
        : Array[Array[Array[Double]]] = {
      for (i <- 0 to t1.size-1) {
        for (j <- 0 to t1(i).size-1) {
          for (k <- 0 to t1(i)(j).size-1) {
            t1(i)(j)(k) += t2(i)(j)(k)
          }
        }
      }
      return t1
    }

    // This isn't used, to minimize tmp Tuple creation.
    def addAccumulator(t1: Array[Array[Array[Double]]], t2: (Int, Byte, Double))
        : Array[Array[Array[Double]]] = {
      //splitcode function pulls out the index to look at.
      // ex: for A|TTA, it pulls out ((111100) ,0) from 00111100
      val splitCode = decodeMarkovChainIndex(t2._2)
      t1(t2._1)(splitCode._1)(splitCode._2) += t2._3
      t1
    }

    def zero(t: Array[Array[Array[Double]]]): Array[Array[Array[Double]]] = {
      return Array.ofDim[Double](t.size, t(0).size, t(0)(0).size)
    }
  }

  def getNumAlignments(fragment: Fragment): Int = {
    return fragment._1.size
  }

  def getAlignmentAttributes(fragment: Fragment, i: Int): (
    Int, Boolean,
    Int, Byte, Array[Byte], Array[Byte], Byte,
    Int, Byte, Array[Byte], Array[Byte], Byte
  ) = {
    val left = fragment._3
    val right = fragment._4
    return (
      fragment._1(i), fragment._2(i),
      left._1(i), left._2(i), left._3(i), left._4(i), left._5(i),
      right._1(i), right._2(i), right._3(i), right._4(i), right._5(i)
    )
  }
  
  def cacheErrorsAndBias(
      fragmentsRDD: RDD[Fragment],
      bcTargSeqs: Broadcast[Array[Array[Byte]]],
      bcTargLens: Broadcast[Array[Int]]
    ): RDD[Fragment] = {
    
    return fragmentsRDD.map { fragment =>
      val numAlignments = getNumAlignments(fragment)
      val readLs = fragment._3
      val readRs = fragment._4
      
      for (i <- 0 until numAlignments) {
        val tid = fragment._1(i)
        val targSeq = bcTargSeqs.value(tid)
        val targLen = bcTargLens.value(tid)

        if (readLs._3(i) != null) {
          readLs._3(i) = getErrorIndices(readLs._3(i), readLs._4(i), targSeq,
                                         readLs._1(i), readLs._2(i), false)
        }
        val biasIndex5 = getBiasIndices(targSeq, targLen, readLs._1(i), false)
        readLs._4(i) = if (i > 0 && biasIndex5._2.sameElements(readLs._4(0))) { null } else { biasIndex5._2 }
        readLs._5(i) = biasIndex5._1

        if (readRs._3(i) != null) {
          readRs._3(i) =  getErrorIndices(readRs._3(i), readRs._4(i), targSeq,
                                          readRs._1(i), readRs._2(i), true)
        }
        val biasIndex3 = getBiasIndices(targSeq, targLen, readRs._1(i), true)
        readRs._4(i) = if (i > 0 && biasIndex3._2.sameElements(readRs._4(0))) { null } else { biasIndex3._2 }
        readRs._5(i) = biasIndex3._1
      }
      fragment
    }
  }

  case class Target(
    val name: String,
    val id: Int,
    val length: Int,
    val seq: Array[Byte]
  ) {

    var bias5: Array[Byte] = null
    var bias3: Array[Byte] = null

    var effLength: Double = _
    var tau: Double = _

    override def toString(): String = {
      return name + "\t" + id + "\t" + length + "\t" + effLength + "\t" + tau + "\n"
    }
  }

  def createRandomArray(size: Int, rangeMax: Int) = (Array.tabulate(size)(_ => rand.nextDouble * rangeMax))

  // Normalizes the array and logs the result (iff shouldLog == true)
  // Use transform?
  def normalizeArray(array: Array[Double], shouldLog: Boolean = false): Array[Double] = {
    val sum = array.reduceLeft(_ + _)
    if (shouldLog) {
      return array.map(elem => math.log(elem / sum))
    } else {
      return array.map(elem => elem / sum)
    }
  }
  
  /**
   * TODO: The following should be part of a TransitionMatrix class ***
   * @param matrix transition matrix
   * @param shouldLog take the log if true
   * @return normalized transition matrix
   */
  def normalizeTransitionMatrix(matrix: Array[Array[Double]], shouldLog: Boolean = false)
      : Array[Array[Double]] = {
    return matrix.map(array => normalizeArray(array, shouldLog))
  }
  
  /**
   * TODO: The following should be part of a MarkovChain class ***
   * @param mc a markov chain represented by Array[transition matrix]
   * @param shouldLog take the log if true
   * @return normalized markov chain
   */
  def normalizeMarkovChain(mc: Array[Array[Array[Double]]], shouldLog: Boolean = false)
      : Array[Array[Array[Double]]] = {
    return mc.map(t_matrix => normalizeTransitionMatrix(t_matrix, shouldLog))
  }
  
  // *** The following should be part of a MarkovChain class ***
  def decodeMarkovChainIndex(code: Byte): (Int, Int) = {
    // Convert to unsigned
    val iCode = code & 0xFF
    // Shift iCode right twice and take lowest 6 bits.
    val conditional = (iCode >> 2) & 0x3F
    // Get lowest two bits.
    val toNuc = iCode & 0x03
    return (conditional, toNuc)
  }
  
  // Finds log(a + b) for x = log(a), y = log(b) without losing "too much" precision
  // This needs to be heavily optimized.
  def logAdd(x: Double, y: Double): Double = {
    if (x == LOG_0) { return y }
    if (y == LOG_0) { return x }
    val u = math.min(x, y)
    val v = math.max(x, y)
    
    val diff = math.exp(v-u)
    if (diff == INF) {
      return v
    }
    
    return u + math.log(1 + diff)
  }

  // Finds log( abs(a - b)/b ) for x = log(a), y = log(b) without losing "too much" precision
  // This needs to be heavily optimized.
  def logRelChange(x: Double, y: Double): Double = {
    if (x == LOG_0) { return LOG_1 }
    if (y == LOG_0) { return INF }
    if (x == y) { return LOG_0 }
    val u = math.min(x, y)
    val v = math.max(x, y)

    return v + math.log(1 - math.exp(u-v)) - y
  }

  def determineConvergence(oldTaus: Array[Double], newTaus: Array[Double]): Boolean = {
    var i = 0
    var haveConverged = true
    while (i < oldTaus.size) {
      val newTau = newTaus(i)
      val oldTau = oldTaus(i)
      val relChange = logRelChange(newTau, oldTau)
      if (newTau > TAU_MIN_THRESHOLD && relChange > MAX_REL_CHANGE) {
        println(i + "," + math.exp(newTau) + "," + math.exp(relChange))
        return false
      }
      i += 1
    }
    return haveConverged
  }

  def determineConvergence2(oldTaus: Array[Double], newTaus: Array[Double], iter: Int): Boolean = {
    var i = 0
    var haveConverged = true
    val toPrint = new ArrayBuffer[String]()
    toPrint += ("====" + iter + "====\n")
    while (i < oldTaus.size) {
      val newTau = newTaus(i)
      val oldTau = oldTaus(i)
      val relChange = logRelChange(newTau, oldTau)
      if (newTau > TAU_MIN_THRESHOLD && relChange > MAX_REL_CHANGE) {
        toPrint += (i + "," + math.exp(newTau) + "," + math.exp(relChange) + "\n")
        haveConverged = false
      }
      i += 1
    }

    // Output to file
    val file = new File("taus-no-convergence.txt")
    if (iter == 1) {
      if (file.exists) file.delete
      file.createNewFile
    }
    val fileWriter = new FileWriter(file.getName, true)
    val writer = new BufferedWriter(fileWriter)
    toPrint.foreach(writer.write(_))
    writer.close()

    return haveConverged
  }

  // Read data file and transform it into a Fragment object.
  def createFragmentsFromProtoBufFile(filePath: String): Array[Fragment] = {
    val fragmentsIter = Deserializer.deserializeFragments(filePath)
    return fragmentsIter.map(extractFragmentFromMsg(_)).toArray
  }

  def extractFragmentFromMsg(fragment: Alignments.Fragment): Fragment = {
    val alignments = fragment.getAlignmentsList()
    val isPaired = fragment.getPaired()
    val targetIds = new ArrayBuffer[Int]()
    val leftFirsts = new ArrayBuffer[Boolean]()
    
    val leftStarts = new ArrayBuffer[Int]()
    val leftLens = new ArrayBuffer[Byte]()
    val mismatchIndicesL = new ArrayBuffer[Array[Byte]]()
    val mismatchNucsL = new ArrayBuffer[Array[Byte]]()

    val rightStarts = new ArrayBuffer[Int]()
    val rightLens = new ArrayBuffer[Byte]()    
    val mismatchIndicesR = new ArrayBuffer[Array[Byte]]()
    val mismatchNucsR = new ArrayBuffer[Array[Byte]]()

    var i = 0

    while (i < alignments.size()) {
      val alignment = alignments.get(i)
      val targetId = alignment.getTargetId()
      var leftFirst = true

      var leftStart = -1
      var leftLen: Byte = 0
      var mismatchIndexL: Array[Byte] = null
      var mismatchNucL: Array[Byte] = null

      var rightStart = -1
      var rightLen: Byte = 0
      var mismatchIndexR: Array[Byte] = null
      var mismatchNucR: Array[Byte] = null

      if (alignment.hasReadL()) {
        val read = alignment.getReadL()
        leftStart = read.getLeftPos()
        leftLen = (read.getRightPos() - leftStart + 1).toByte
        if (read.hasMismatchIndices() && read.hasMismatchNucs()) {
          mismatchIndexL = read.getMismatchIndices().toByteArray()
          mismatchNucL = read.getMismatchNucs().toByteArray()
        }
        leftFirst = read.getFirst()
      }
      if (alignment.hasReadR()) {
        val read = alignment.getReadR()
        rightStart = read.getRightPos()
        rightLen = (rightStart - read.getLeftPos() + 1).toByte
        if (read.hasMismatchIndices() && read.hasMismatchNucs()) {
          mismatchIndexR = read.getMismatchIndices().toByteArray()
          mismatchNucR = read.getMismatchNucs().toByteArray()
        }
        leftFirst = !read.getFirst()
      }

      targetIds.append(targetId)
      leftFirsts.append(leftFirst)

      leftStarts.append(leftStart)
      leftLens.append(leftLen)
      mismatchIndicesL.append(mismatchIndexL)
      mismatchNucsL.append(mismatchNucL)

      rightStarts.append(rightStart) 
      rightLens.append(rightLen)
      mismatchIndicesR.append(mismatchIndexR)
      mismatchNucsR.append(mismatchNucR)

      i += 1
    }

    return (
      targetIds.toArray,
      leftFirsts.toArray,
      (leftStarts.toArray,
        leftLens.toArray,
        mismatchIndicesL.toArray,
        mismatchNucsL.toArray,
        Array.fill[Byte](alignments.size())(-1)),
      (rightStarts.toArray,
        rightLens.toArray,
        mismatchIndicesR.toArray,
        mismatchNucsR.toArray,
        Array.fill[Byte](alignments.size())(-1))
    )
  }

  def createFragment(encoding: String): Fragment = {
    val fragment = Deserializer.deserializeFragment(encoding)
    return extractFragmentFromMsg(fragment)
  }

  // Parse file containing target names and lengths
  def createTargets(filePath: String): Array[Target] = {
    val targetMsgIter = Deserializer.deserializeTargets(filePath)
    val targets = new ArrayBuffer[Target]()

    for (targetMsg <- targetMsgIter) {
      targets.append(
        Target(
          targetMsg.getName(),
          targetMsg.getId(),
          targetMsg.getLength(),
          targetMsg.getSeq().toByteArray()))
    }

    return targets.toArray
  }

  // Returns reference to 'obs', with new weights set.
  def updateBiasMarkovChain(
      observed: Array[Array[Array[Double]]], expected: Array[Array[Array[Double]]]
    ) = {
    var i = 0
    while (i < observed.size) {
      var j = 0
      while (j < observed(i).size) {
        var k = 0
        while (k < observed(i)(j).size) {
          observed(i)(j)(k) = observed(i)(j)(k) - expected(i)(j)(k)
          k += 1
        }
        j += 1
      }
      i += 1
    }
    observed
  }

  // Update bias params.
  // rhos (unlogged taus) and flCmf are not logged
  // biasObs_5 and biasObs_3 are logged
  def updateBias(
      sc: SparkContext,
      targetsRDD: RDD[Target],
      rhos: Array[Double],
      flCmf: Array[Double],
      biasObs5: Array[Array[Array[Double]]],
      biasObs3: Array[Array[Array[Double]]],
      oldBiasWeight5: Array[Array[Array[Double]]],
      oldBiasWeight3: Array[Array[Array[Double]]]
    ): (Array[Array[Array[Double]]], Array[Array[Array[Double]]], Array[Double]) = {

    var bcRhos: Broadcast[Array[Double]] = sc.broadcast(rhos)
    var bcFlCmf: Broadcast[Array[Double]] = sc.broadcast(flCmf)
    var bcOldBias5: Broadcast[Array[Array[Array[Double]]]] = sc.broadcast(oldBiasWeight5)
    var bcOldBias3: Broadcast[Array[Array[Array[Double]]]] = sc.broadcast(oldBiasWeight3)

    val accBiasExp5: Accumulable[Array[Array[Array[Double]]], (Int, Byte, Double)] = sc.accumulable(
      Array.fill[Double](BIAS_MATRIX_SIZE, math.pow(4, BIAS_ORDER).toInt, 4) (1.0)
    )
    val accBiasExp3: Accumulable[Array[Array[Array[Double]]], (Int, Byte, Double)] = sc.accumulable(
      Array.fill[Double](BIAS_MATRIX_SIZE, math.pow(4, BIAS_ORDER).toInt, 4) (1.0)
    )
    val accAvgTargetBias: Accumulable[Array[Double], (Int, Double)] = sc.accumulable(new Array[Double](rhos.size))

    println("==============")
    println("updating bias")
    println("==============")

    println("num partitions: " + targetsRDD.partitions.size)

    targetsRDD.foreach { target =>
      val rho = rhos(target.id)

      var totalBias5 = LOG_0
      var totalBias3 = LOG_0
      var totalMass = LOG_0

      val biasIndex5 = new LinkedList[Byte]()
      val biasIndex3 = new LinkedList[Byte]()

      var i = 0
      while (i < BIAS_WINDOW) {
        biasIndex5.addLast(extractNuc(target.seq, i, false))
        biasIndex3.addLast(extractNuc(target.seq, target.length-i-1, true))
        i += 1
      }

      i = 0
      while (i < target.length) {
        val distFromEnd = target.length - i
        val offSet = math.max(0, BIAS_WINDOW - i)

        var mass = 1.0
        if (distFromEnd <= MAX_FRAG_LENGTH) {
          mass *= bcFlCmf.value(distFromEnd)
        }

        var weight5 = LOG_1
        var weight3 = LOG_1

        if (i > BIAS_WINDOW) {
          biasIndex5.poll()
          biasIndex3.poll()
        }
        if (i + BIAS_WINDOW < target.length) {
          biasIndex5.addLast((((biasIndex5.getLast() << 2) & 0xFF)
                              + extractNuc(target.seq, i+BIAS_WINDOW, false)).toByte)
          biasIndex3.addLast((((biasIndex3.getLast() << 2) & 0xFF)
                              + extractNuc(target.seq, target.length-(i+BIAS_WINDOW)-1, true)).toByte)
        }
        var j = offSet
        biasIndex5.foreach { indRaw =>
          val ind = if (j < BIAS_ORDER) (indRaw & 255 >> (2*(BIAS_ORDER-j))).toByte else indRaw
          val splitCode = decodeMarkovChainIndex(ind)
          weight5 += bcOldBias5.value(j)(splitCode._1)(splitCode._2)
          accBiasExp5 += (j, ind, rho * mass)
          j += 1
        }
        j = offSet
        biasIndex3.foreach { indRaw =>
          val ind = if (j < BIAS_ORDER) (indRaw & 255 >> (2*(BIAS_ORDER-j))).toByte else indRaw
          val splitCode = decodeMarkovChainIndex(ind)
          weight3 += bcOldBias3.value(j)(splitCode._1)(splitCode._2)
          accBiasExp3 += (j, ind, rho * mass)
          j += 1
        }

        val logMass = math.log(mass)
        totalBias5 = logAdd(totalBias5, weight5 + logMass)
        totalBias3 = logAdd(totalBias3, weight3 + logMass)
        totalMass = logAdd(totalMass, logMass)

        i += 1
      }
      val avgBias5 = totalBias5 - totalMass
      val avgBias3 = totalBias3 - totalMass
      accAvgTargetBias += (target.id, avgBias5 + avgBias3)
    }

    // Collected accumulated values, normalize, and log
    val biasExp5 = normalizeMarkovChain(accBiasExp5.value, true)
    val biasExp3 = normalizeMarkovChain(accBiasExp3.value, true)

    // Compute new bias weights.
    val biasWeights5 = updateBiasMarkovChain(biasObs5, biasExp5)
    val biasWeights3 = updateBiasMarkovChain(biasObs3, biasExp3)

    // Drop broadcast variable blocks from memory.
    SparkMemoryUtilities.dropBroadcastVar(bcRhos)
    SparkMemoryUtilities.dropBroadcastVar(bcFlCmf)
    SparkMemoryUtilities.dropBroadcastVar(bcOldBias5)
    SparkMemoryUtilities.dropBroadcastVar(bcOldBias3)

    //println("biasWeights5: " + stringOf(biasWeights5(0)))
    //println("biasWeights3: " + stringOf(biasWeights3(0)))
    
    return (biasWeights5, biasWeights3, accAvgTargetBias.value)
  }

  // Logged effective length, calculated with a normalized, un-logged FLD
  def computeEffLen(length: Int, fld: Array[Double]): Double = {
    var effLength: Double = LOG_1
    val maxLength = math.min(length, MAX_FRAG_LENGTH)
    var i = 1
    while(i <= maxLength) {
      effLength += fld(i) * (length - i + 1)
      i += 1
    }
    return math.log(effLength)
  }

  // Compute CMF for given distributions.
  def computeCmfs(pmfs: Array[Double]): Array[Double] = {
    val cmfs = new Array[Double](pmfs.size)
    val pmfSum = pmfs.sum
    cmfs(0) = pmfs(0)
    var i = 1
    while (i < cmfs.size) {
      cmfs(i) = cmfs(i - 1) + pmfs(i) / pmfSum
      i += 1
    }
    return cmfs
  }

  def resetTaus(taus: Accumulable[Array[Double], (Int, Double)]) {
    var i = 0
    val value = taus.value
    while (i < value.size) {
      value(i) = 0.0
      i += 1
    }
  }

  def resetFld(fld: Accumulable[Array[Double], (Int, Double)]) {
    var i = 0
    val value = fld.value
    while (i < value.size) {
      value(i) = 1.0
      i += 1
    }
  }

  def resetMarkov(markovChainAcc: Accumulable[Array[Array[Array[Double]]], (Int, Byte, Double)]) {
    var i = 0
    val value = markovChainAcc.value
    while (i < value.size) {
      var j = 0
      while (j < value(i).size) {
        var k = 0
        while (k < value(i)(j).size) {
          value(i)(j)(k) = 1.0
          k += 1
        }
        j += 1
      }
      i += 1
    }
  }

  def extractNuc(seq: Array[Byte], index: Int, doComplement: Boolean): Byte = {
    val i = index / 4
    val j = index % 4
    var nuc = (seq(i) >> (j*2)) & 0x3
    if (doComplement) {
      nuc ^= 3
    }
    return nuc.toByte
  }

  // Indels are not allowed for now!
  def getErrorIndices(
    mismatchIndices: Array[Byte],
    mismatchNucs: Array[Byte],
    ref: Array[Byte],
    readStart: Int,
    readLen: Byte,
    rev: Boolean
  ): Array[Byte] = {
    val errorIndices = new ArrayBuffer[Byte]()
    val sign = if (rev) -1 else 1
    var lastNuc:Byte = 0
    var j = 0
    var i = 0
    while (i < readLen) {
      val refNuc = extractNuc(ref, sign*i + readStart, rev)
      var obsNuc = refNuc
      if (j < mismatchIndices.size && mismatchIndices(j) == i) {
        obsNuc = extractNuc(mismatchNucs, j, false)
        j += 1
      }
      errorIndices += ((lastNuc << 4) + (refNuc << 2) + obsNuc).toByte
      lastNuc = obsNuc
      i += 1
    }
    return errorIndices.toArray
  }
  
  def getBiasIndices(
    ref: Array[Byte],
    refLen: Int,
    readStart: Int,
    rev: Boolean
  ): (Byte, Array[Byte]) = {
    val biasIndices = new ArrayBuffer[Byte]()
    val sign = if (rev) -1 else 1
    
    var windowStart = math.max(0, readStart - BIAS_WINDOW)
    var windowStop = math.min(refLen - 1, readStart + BIAS_WINDOW)
    
    if (rev) {
      val tmp = windowStart
      windowStart = windowStop
      windowStop = tmp
    }
    
    var lastNuc:Byte = 0
    if (!rev && windowStart == 0 && readStart != BIAS_WINDOW) {
      while (windowStart < BIAS_ORDER) {
        val refNuc = extractNuc(ref, windowStart, rev)
        lastNuc = (((lastNuc << 2) & 0xFF) + refNuc).toByte
        windowStart += 1
      }
    }
    if (rev && windowStart == refLen - 1 && readStart != refLen - BIAS_WINDOW - 1) {
      while (windowStart > refLen - (BIAS_ORDER+1)) {
        val refNuc = extractNuc(ref, windowStart, rev)
        lastNuc = (((lastNuc << 2) & 0xFF) + refNuc).toByte
        windowStart -= 1
      }
    }

    var i = windowStart - sign
    do {
      i += sign
      val refNuc = extractNuc(ref, i, rev)
      lastNuc = (((lastNuc << 2) & 0xFF) + refNuc).toByte
      biasIndices += lastNuc
    } while (i != windowStop) 

    var windowOffset = 0
    if (rev) {
      windowOffset = BIAS_WINDOW - (windowStart - readStart)
    } else {
      windowOffset = BIAS_WINDOW - (readStart - windowStart)
    }
    return (windowOffset.toByte, biasIndices.toArray)
  }

  // Output format: <targetName> <Id> <length> <effLength> <tau>
  def outputToLocalFile(targets: Array[Target], filePath: String) {
    val fos = new FileOutputStream(filePath)
    val writer = new BufferedWriter(new OutputStreamWriter(fos))
    targets.foreach { target =>
      writer.write(target.toString)
    }
    writer.close()
  }

  // Run EM
  def runExpress(
      bcTargSeqs: Broadcast[Array[Array[Byte]]],
      bcTargLens: Broadcast[Array[Int]],
      targets: Array[Target],
      fragmentsRDD: RDD[Fragment],
      iterations: Int,
      shouldUseBias: Boolean,
      isLocal: Boolean
    ): RDD[Target] = {

    val sc = fragmentsRDD.context

    println("running for " + iterations + " iterations")

    var targetParallelism = System.getProperty("express.targetParallelism", "10").toInt
    println("making targetsRDD with " + targetParallelism + " partitions")
    val targetsRDD: RDD[Target] =
      sc.parallelize(targets.toSeq, targetParallelism).persist(StorageLevel.MEMORY_AND_DISK)

    // Initialize parameters
    val numAlignments = targets.size
    val loggedLengths = targets.map(target => math.log(target.length))

    // Abundances, initialize as a value proportional to the length
    val initTaus = loggedLengths

    // Fragment lengths, intitialize to be uniform
    val initFld: Array[Double] = new Array[Double](MAX_FRAG_LENGTH + 1)

    // Error Markov chains for the first and second read of the pair.
    val initErrors1: Array[Array[Array[Double]]] =
      Array.tabulate[Double](MAX_READ_LENGTH, math.pow(4, ERROR_ORDER).toInt, 4) { (i,j,k) =>
        if ((j & 3) == k) {math.log(0.97)} else {math.log(0.01)}
      }
    val initErrors2: Array[Array[Array[Double]]] =
      Array.tabulate[Double](MAX_READ_LENGTH, math.pow(4, ERROR_ORDER).toInt, 4) { (i,j,k) =>
        if ((j & 3) == k) {math.log(0.97)} else {math.log(0.01)}
      }

    // Bias
    // Two sets of 21x64x4
    val initBias5: Array[Array[Array[Double]]] =
      Array.fill[Double](BIAS_MATRIX_SIZE, math.pow(4, BIAS_ORDER).toInt, 4) (LOG_1)
    val initBias3: Array[Array[Array[Double]]] =
      Array.fill[Double](BIAS_MATRIX_SIZE, math.pow(4, BIAS_ORDER).toInt, 4) (LOG_1)

    // Initial broadcast to slaves
    var bcEffLengths: Broadcast[Array[Double]] = sc.broadcast(loggedLengths)
    var bcTaus: Broadcast[Array[Double]] = sc.broadcast(initTaus)
    var bcFld: Broadcast[Array[Double]] = sc.broadcast(initFld)
    var bcErrors1: Broadcast[Array[Array[Array[Double]]]] = sc.broadcast(initErrors1)
    var bcErrors2: Broadcast[Array[Array[Array[Double]]]] = sc.broadcast(initErrors2)
    var bcBias5: Broadcast[Array[Array[Array[Double]]]] = sc.broadcast(initBias5)
    var bcBias3: Broadcast[Array[Array[Array[Double]]]] = sc.broadcast(initBias3)

    // Parameters to update. We can reuse accumulables since local/slave values are cleared before a task is run.
    // These accumulables are used for learning in each round/iteration.
    var newTaus: Accumulable[Array[Double], (Int, Double)] = sc.accumulable(new Array[Double](numAlignments))
    var newFld: Accumulable[Array[Double], (Int, Double)] = sc.accumulable(
        Array.fill(MAX_FRAG_LENGTH + 1)(1.0)
      )
    var newErrors1: Accumulable[Array[Array[Array[Double]]], (Int, Byte, Double)] = sc.accumulable(
        Array.fill[Double](MAX_READ_LENGTH, math.pow(4, ERROR_ORDER).toInt, 4) (1.0)
      )
    var newErrors2: Accumulable[Array[Array[Array[Double]]], (Int, Byte, Double)] = sc.accumulable(
        Array.fill[Double](MAX_READ_LENGTH, math.pow(4, ERROR_ORDER).toInt, 4) (1.0)
      )
    var newBias5: Accumulable[Array[Array[Array[Double]]], (Int, Byte, Double)] =
      if (shouldUseBias) {
        sc.accumulable(
          Array.fill[Double](BIAS_MATRIX_SIZE, math.pow(4, BIAS_ORDER).toInt, 4) (1.0) )
      } else null
    var newBias3: Accumulable[Array[Array[Array[Double]]], (Int, Byte, Double)] =
      if (shouldUseBias) {
        sc.accumulable(
          Array.fill[Double](BIAS_MATRIX_SIZE, math.pow(4, BIAS_ORDER).toInt, 4) (1.0) )
      } else null

    // Function that sets results (the Target effLength and taus).
    // Called on Targets before writing to disk and returning output RDD.
    val transformFn = (target: Target, bcTaus: Broadcast[Array[Double]]) => {
      val tid = target.id
      // Exponentiate to get out of log space
      target.effLength = math.exp(bcEffLengths.value(tid))
      target.tau = bcTaus.value(tid)
      target
    }

    // For debugging
    var sumOfTaus = new ArrayBuffer[Double]()
    var sumOfFragmentsSize = new ArrayBuffer[Double]()

    var iter = 1
    var haveConverged = false

    var iterTime = System.currentTimeMillis()

    while (!haveConverged && iter <= iterations) {
    
      var currentIterTime = System.currentTimeMillis()
      println("Iter: " + iter + ", time: " + (currentIterTime - iterTime))
      
      // Reset accumulable values.
      resetTaus(newTaus)
      resetFld(newFld)
      resetMarkov(newErrors1)
      resetMarkov(newErrors2)
      if (shouldUseBias) {
        resetMarkov(newBias3)
        resetMarkov(newBias5)
      }

      var shouldUpdateAllParams = (iter <= BURN_IN_ITERATIONS ||
            (iter % UPDATE_INTERVAL == 0 && iter <= MAX_UPDATE_ITERATIONS))

      // Iterate through cached reads.
      val processedRDD = fragmentsRDD.mapPartitionsWithIndex { case(index, fragmentsPart) =>
        val errorIndices1 = if (shouldUpdateAllParams) new ObjectArrayList[Array[Byte]]() else null
        val errorIndices2 = if (shouldUpdateAllParams) new ObjectArrayList[Array[Byte]]() else null
        val biasIndices5 = if (shouldUpdateAllParams && shouldUseBias) new ArrayBuffer[(Byte, Array[Byte])]() else null
        val biasIndices3 = if (shouldUpdateAllParams && shouldUseBias) new ArrayBuffer[(Byte, Array[Byte])]() else null

        var likelihoods = new DoubleArrayList()

        println("Iter: " + iter + ", Part: " + index)

        val newPart = fragmentsPart.map { fragment =>
          val numAlignments = getNumAlignments(fragment)
          likelihoods.clear
          likelihoods.size(numAlignments)
          var totLikelihood = LOG_0

          if (shouldUpdateAllParams) {
            errorIndices1.clear
            errorIndices2.clear

            if (shouldUseBias) {
              biasIndices5.clear
              biasIndices3.clear
            }
          }

          var refErrLikelihood1 = LOG_1
          var refErrLikelihood2 = LOG_1
          var refBiasLikelihood5 = LOG_1
          var refBiasLikelihood3 = LOG_1

          val left = fragment._3
          val right = fragment._4

          for (i <- 0 until numAlignments) {
            val pairTargetId = fragment._1(i)
            val leftFirst = fragment._2(i)
            val leftStart = left._1(i)
            val leftLen = left._2(i)
            val misc1L = left._3(i)
            val misc2L = left._4(i)
            val misc3L = left._5(i)
            val rightStart = right._1(i)
            val rightLen = right._2(i)
            val misc1R = right._3(i)
            val misc2R = right._4(i)
            val misc3R = right._5(i)

            val pairLength = Math.abs(rightStart - leftStart) + 1

            val bcTausValue = bcTaus.value(pairTargetId)
            val bcFldValue = bcFld.value(pairLength)
            val bcEffLengthsValue = bcEffLengths.value(pairTargetId)

            likelihoods.set(i,
              bcTausValue + bcFldValue - bcEffLengthsValue
            )

            val targSeq = bcTargSeqs.value(pairTargetId)
            val targLen = bcTargLens.value(pairTargetId)

            var errorIndex1 =
              if (misc3L != -1 || misc1L == null) {
                misc1L
              } else {
                getErrorIndices(misc1L, misc2L, targSeq, leftStart, leftLen, false)
              }
            var errorIndex2 =
              if (misc3R != -1 || misc1R == null) {
                misc1R
              } else {
                getErrorIndices(misc1R, misc2R, targSeq, rightStart, rightLen, true)
              }

            if (!leftFirst) {
              val tmp = errorIndex1
              errorIndex1 = errorIndex2
              errorIndex2 = tmp
            }

            if (shouldUpdateAllParams) {
              errorIndices1.add(if (errorIndex1 == null) { if (errorIndices1.size > 0) { errorIndices1.get(0) } else { errorIndex1 } } else { errorIndex1 })
              errorIndices2.add(if (errorIndex2 == null) { if (errorIndices2.size > 0) { errorIndices2.get(0) } else { errorIndex2 } } else { errorIndex2 })
            }

            // Sum current likelihood with values in errors likelihood table at MAX_READ_LENGTH indices.
            var errLikelihood1 = LOG_1
            if (i > 0 && errorIndex1 == null) {
              errLikelihood1 = refErrLikelihood1
            } else if ( errorIndex1 == null ) {
              //TODO: what to do here?  was throwing NPE here -aday
            } else {
              for (j <- 0 until errorIndex1.size) {
                val splitCode = decodeMarkovChainIndex(errorIndex1(j))
                errLikelihood1 += bcErrors1.value(j)(splitCode._1)(splitCode._2)
              }
              if (i == 0) { refErrLikelihood1 = errLikelihood1 }
            }
            likelihoods.set(i, likelihoods.get(i) + errLikelihood1)
            
            var errLikelihood2 = LOG_1
            if (i > 0 && errorIndex2 == null) {
              errLikelihood2 = refErrLikelihood2
            } else if ( errorIndex2 == null ) {
              //TODO: what to do here?  was throwing NPE here -aday
            } else {
              for (j <- 0 until errorIndex2.size) {
                val splitCode = decodeMarkovChainIndex(errorIndex2(j))
                errLikelihood2 += bcErrors2.value(j)(splitCode._1)(splitCode._2)
              }
              if (i == 0) { refErrLikelihood2 = errLikelihood2 }
            }
            likelihoods.set(i, likelihoods.get(i) + errLikelihood2)

            // Sum (multiply) current likelihood with values in bias likelihood table at
            // BIAS_MATRIX_SIZE indices.
            if (shouldUseBias) {
              // Construct bias indices
              val biasIndex5 =
                if (misc3L != -1) {
                  (misc3L, misc2L)
                } else {
                  getBiasIndices(targSeq, targLen, leftStart, false)
                }
              val biasIndex3 =
                if (misc3R != -1) {
                  (misc3R, misc2R)
                } else {
                  getBiasIndices(targSeq, targLen, rightStart, true)
                }

              if (shouldUpdateAllParams) {
                biasIndices5 += (if (biasIndex5._2 == null) { biasIndices5(0) } else { biasIndex5 })
                biasIndices3 += (if (biasIndex3._2 == null) { biasIndices3(0) } else { biasIndex3 })
              }

              var biasLikelihood5 = LOG_1
              if (i > 0 && biasIndex5._2 == null) {
                biasLikelihood5 = refBiasLikelihood5
              } else {
                for (j <- 0 until biasIndex5._2.size) {
                  val splitCode = decodeMarkovChainIndex(biasIndex5._2(j))
                  biasLikelihood5 += bcBias5.value(j + biasIndex5._1)(splitCode._1)(splitCode._2)
                }
                if (i == 0) { refBiasLikelihood5 = biasLikelihood5 }
              }
              likelihoods.set(i, likelihoods.get(i) + biasLikelihood5)

              var biasLikelihood3 = LOG_1
              if (i > 0 && biasIndex3._2 == null) {
                biasLikelihood3 = refBiasLikelihood3
              } else {
                for (j <- 0 until biasIndex3._2.size) {
                  val splitCode = decodeMarkovChainIndex(biasIndex3._2(j))
                  biasLikelihood3 += bcBias3.value(j + biasIndex3._1)(splitCode._1)(splitCode._2)
                }
                if (i == 0) { refBiasLikelihood3 = biasLikelihood3 }
              }
              likelihoods.set(i, likelihoods.get(i) + biasLikelihood3)
            }
            totLikelihood = logAdd(totLikelihood, likelihoods.get(i))
          }
        
          var newTotLikelihood = 0.0
          // Normalize likelihood by total likelihood, exponentiate to get out of log space.
          for (i <- 0 until numAlignments) {
            likelihoods.set(i, math.exp(likelihoods.get(i) - totLikelihood))
            newTotLikelihood += likelihoods.get(i)
          }

          // Update parameters
          for (i <- 0 until numAlignments) {
            val pairTargetId = fragment._1(i)
            val leftFirst = fragment._2(i)
            val leftStart = left._1(i)
            val leftLen = left._2(i)
            val misc1L = left._3(i)
            val misc2L = left._4(i)
            val misc3L = left._5(i)
            val rightStart = right._1(i)
            val rightLen = right._2(i)
            val misc1R = right._3(i)
            val misc2R = right._4(i)
            val misc3R = right._5(i)

            val pairLength = Math.abs(rightStart - leftStart) + 1

            // Correct for numerical issues
            val p = likelihoods.get(i) / newTotLikelihood

            newTaus.localValue(pairTargetId) += p

            if (shouldUpdateAllParams) {
              val errorIndex1 = errorIndices1.get(i)
              val errorIndex2 = errorIndices2.get(i)

              newFld.localValue(pairLength) += p
              if ( errorIndex1 != null ) { //TODO is this right? avert NPE -aday
                for (j <- 0 until errorIndex1.size) {
                  val splitCode = decodeMarkovChainIndex(errorIndex1(j))
                  newErrors1.localValue(j)(splitCode._1)(splitCode._2) += p
                }
              }
              if ( errorIndex2 != null ) { //TODO is this right? avert NPE -aday
                for (j <- 0 until errorIndex2.size) {
                  val splitCode = decodeMarkovChainIndex(errorIndex2(j))
                  newErrors2.localValue(j)(splitCode._1)(splitCode._2) += p
                }
              }

              if (shouldUseBias) {
                // Do the same for bias.
                val biasIndex5 = biasIndices5(i)
                val biasIndex3 = biasIndices3(i)

                for (j <- 0 until biasIndex5._2.size) {
                  val splitCode = decodeMarkovChainIndex(biasIndex5._2(j))
                  newBias5.localValue(j + biasIndex5._1)(splitCode._1)(splitCode._2) += p
                }
                for (j <- 0 until biasIndex3._2.size) {
                  val splitCode = decodeMarkovChainIndex(biasIndex3._2(j))
                  newBias3.localValue(j + biasIndex3._1)(splitCode._1)(splitCode._2) += p
                }
              }
            }
          }
         fragment
        }
        println("finishing iter: " + iter + ", Part: " + index + ", time: " + System.currentTimeMillis())
        newPart
      }

      val oldTaus = bcTaus.value

      // ===== Debugging =====
      // Print out some memory usage info.
      var accumFragmentsSize = sc.accumulable(0.0)

      if (true) {
        processedRDD.mapPartitions{ partition =>
          var partRef = partition

          if (iter == -1) {
            println("\n ======= Gathering Memory Usge Info For Partition ===========")
            val partArr = partRef.toArray

            println("num Fragments: " + partArr.asInstanceOf[Array[Any]].size / 2)
            val fragSize = SparkMemoryUtilities.estimateSize(partArr)
            println("Total size for Fragments: " + fragSize)
            accumFragmentsSize += fragSize
            println("effLengths: " + SparkMemoryUtilities.estimateSize(bcEffLengths.value))
            println("bcTaus: " + SparkMemoryUtilities.estimateSize(bcTaus.value))
            println("bcFld: " + SparkMemoryUtilities.estimateSize(bcFld.value))
            println("bcErrors1: " + SparkMemoryUtilities.estimateSize(bcErrors1.value))
            println("bcErrors2: " + SparkMemoryUtilities.estimateSize(bcErrors2.value))
            println("\n ============== Done Gathering Memory Usage Info ============")

            partRef = partArr.iterator
          }
          partRef
        }.foreach(_ => Unit)
      }

      targetsRDD.mapPartitions { partition =>
        if (!isLocal)  {
          SparkMemoryUtilities.dropBroadcastVar(bcTaus)

          if (shouldUpdateAllParams) {
            SparkMemoryUtilities.dropBroadcastVar(bcEffLengths)
            SparkMemoryUtilities.dropBroadcastVar(bcFld)
            SparkMemoryUtilities.dropBroadcastVar(bcErrors1)
            SparkMemoryUtilities.dropBroadcastVar(bcErrors2)

            if (shouldUseBias) {
              SparkMemoryUtilities.dropBroadcastVar(bcBias5)
              SparkMemoryUtilities.dropBroadcastVar(bcBias3)
            }
          }
        }
        partition
      }.foreach(_ => Unit)

      println("Accumulating counts...")
      // Log if we're not done iterating. Always false if iterations are not specified by user.
      val shouldLog = (iter != iterations)
      // Note: newTaus.value isn't exponentiated
      val newNormalizedTaus = if (shouldLog) normalizeArray(newTaus.value, true) else newTaus.value

      // Stop if we've converged
      //haveConverged = (iter > BURN_IN_ITERATIONS && determineConvergence(oldTaus, newNormalizedTaus, iter))
      val conv = determineConvergence(oldTaus, newNormalizedTaus)
      println("conv: " + conv)
      haveConverged = iter > BURN_IN_ITERATIONS && conv


      // Drop old bcTaus from memory
      SparkMemoryUtilities.dropBroadcastVar(bcTaus)
      bcTaus = if (haveConverged) {
          sc.broadcast(newTaus.value)
        } else {
          sc.broadcast(newNormalizedTaus)
        }

      if (shouldUpdateAllParams) {
        println("Updating auxiliary parameters...")
        // Drop old broadcast variables from memory.
        // Note: until Spark is patched, these and getBlockId() will need to be in a separate object/file
        // that is in 'package spark', since HttpBroadcast is a 'package private' class.
        SparkMemoryUtilities.dropBroadcastVar(bcEffLengths)
        SparkMemoryUtilities.dropBroadcastVar(bcFld)
        SparkMemoryUtilities.dropBroadcastVar(bcErrors1)
        SparkMemoryUtilities.dropBroadcastVar(bcErrors2)
        
        val normalizedFld = normalizeArray(newFld.value, false)

        val lognormFld = normalizeArray(newFld.value, shouldLog)
        bcFld = sc.broadcast(lognormFld)

        // >> Normalize columns of matrices and log them.
        val lognormErrors1 = normalizeMarkovChain(newErrors1.value, shouldLog)
        bcErrors1 = sc.broadcast(lognormErrors1)

        val lognormErrors2 = normalizeMarkovChain(newErrors2.value, shouldLog)
        bcErrors2 = sc.broadcast(lognormErrors2)

        // Don't broadcast until after we (optionally) update bias
        var effLengths = targetsRDD.map(target =>
          computeEffLen(target.length, normalizedFld)).collect()

        // Update bias
        if (shouldUseBias) {
          val rhos = effLengths.zipWithIndex.map { case (effLength, i) => newTaus.value(i) / math.exp(effLength) }
          val (biasWeights5, biasWeights3, avgTargetBias) =
            updateBias(sc, targetsRDD, rhos, computeCmfs(newFld.value),
              normalizeMarkovChain(newBias5.value, true), normalizeMarkovChain(newBias3.value, true),
              bcBias5.value, bcBias3.value)
         
          SparkMemoryUtilities.dropBroadcastVar(bcBias5)
          SparkMemoryUtilities.dropBroadcastVar(bcBias3)
          bcBias5 = sc.broadcast(biasWeights5)
          bcBias3 = sc.broadcast(biasWeights3)
          
          effLengths = Array.tabulate[Double](effLengths.size){ i => effLengths(i) + avgTargetBias(i) }
        }
        bcEffLengths = sc.broadcast(effLengths)
      }

      if (DEBUG) {
        sumOfTaus.append(newTaus.value.reduceLeft(_+_))
        sumOfFragmentsSize.append(accumFragmentsSize.value)
      }

      if (iter == 20) {
        println("time for 20 iters: " + (System.currentTimeMillis - runExpressStartTime))
      } else if (iter == 120) {
        println("time for 120 iters: " + (System.currentTimeMillis - runExpressStartTime))
      }

      if (haveConverged) {
        println("have converged")
      }
      if (iter % ExpressEnv.OUTPUT_INTERVAL == 0) {
        println("reached outputInterval")
      }
      if (iterations == iter) {
        println("iterations reached")
      }

      if ((ExpressEnv.OUTPUT_INTERVAL != -1 &&
           iter > 1 && iter % ExpressEnv.OUTPUT_INTERVAL == 0) ||
          haveConverged ||
          iterations == iter) {
        // Format of output is tab-delimited <targetName> <Id> <length> <effLength> <tau>
        val outputFilePath = "%shits.%d.%d.%s.results".format(
            System.getProperty("express.output.directory", System.getProperty("user.dir")) + "/",
            iter,
            System.currentTimeMillis - runExpressStartTime,
            if (shouldUseBias) "withBias" else "withoutBias"
          )
        println("============================================")
        println("Outputting results at iteration " + iter + " to:\n" + outputFilePath)
        println("============================================")

        // Slight memory leak, but should be alright, since this doesn't happen often.
        val bcNewTaus = sc.broadcast(newTaus.value)
        outputToLocalFile(targets.map(transformFn(_, bcNewTaus)), outputFilePath)
      }

      iter += 1
    }

    if (DEBUG) {
      val normalizedFld = bcFld.value
      val errors1 = bcErrors1.value
      val errors2 = bcErrors2.value

      val printDivider = "\n\n" + "="*50 + "\n" + "="*50

      //println("Normalized Fld: \n + " + stringOf(normalizedFld) + printDivider)
      //println("Errors1: \n" + stringOf(errors1) + printDivider)
      //println("Errors2: \n" + stringOf(errors2) + printDivider)
      println("Sum of taus for each iteration: ")
      sumOfTaus.zipWithIndex.foreach( tup => print(tup + "; "))
      println(printDivider)
      println("Sum of fragmentRDD sizes for each iteration: ")
      sumOfFragmentsSize.zipWithIndex.foreach(tup => print(tup + "; "))
      println(printDivider)
    }

    // This isn't used right now, but could be by downstream operators.
    return targetsRDD.map(transformFn(_, bcTaus))
  }
}
