package expressd

import java.io.{BufferedReader, EOFException, FileInputStream, InputStream, InputStreamReader}

import org.apache.commons.codec.binary.Base64

import expressd._
import expressd.protobuf._
import Alignments._
import Targets._

/*
 * A deserializer for decoding strings stored in a file.
 * Currently, this is just base64
 */
object Deserializer {

  def deserializeFile(filePath: String): Iterator[Array[Byte]] = {
  	new Iterator[Array[Byte]] {
      val fis = new FileInputStream(filePath)
      val br = new BufferedReader(new InputStreamReader(fis))
  	  var finished = false
  	  var nextValue: Array[Byte] = null
      var bufferedValue: Array[Byte] = decodeNextLine()
 
      def decodeNextLine(): Array[Byte] = {
        val base64Bytes = br.readLine()
        if (base64Bytes == null) finished = true
        return Base64.decodeBase64(base64Bytes)
      }

  	  override def hasNext = !finished

  	  override def next(): Array[Byte] = {
          if (finished) {
            throw new NoSuchElementException("End of stream")
          }
          nextValue = bufferedValue
          bufferedValue = decodeNextLine()
          if (finished) closeStream()
          return nextValue
  	  }

      def closeStream() { fis.close() }
    }
  }

  def deserializeFragments(filePath: String): Iterator[Fragment] = {
    new Iterator[Fragment] {
      val linesIter = deserializeFile(filePath)

      override def hasNext = linesIter.hasNext

      override def next(): Fragment = {
        if (!linesIter.hasNext) {
          throw new NoSuchElementException("End of stream")
        }
        val nextFrag = Fragment.parseFrom(linesIter.next())
        return nextFrag
      }
    }
  }

  def deserializeTargets(filePath: String): Iterator[Target] = {
    new Iterator[Target] {
      val linesIter = deserializeFile(filePath)

      override def hasNext = linesIter.hasNext

      override def next(): Target = {
        if (!linesIter.hasNext) {
          throw new NoSuchElementException("End of stream")
        }
        val nextTarget = Target.parseFrom(linesIter.next())
        return nextTarget
      }
    }
  }

  def deserializeFragment(encoding: String): Fragment = {
    val decodedBytes = Base64.decodeBase64(encoding)
    return Fragment.parseFrom(decodedBytes)
  }

  def deserializeTarget(encoding: String): Target = {
    val decodedBytes = Base64.decodeBase64(encoding)
    return Target.parseFrom(decodedBytes)
  }
}
