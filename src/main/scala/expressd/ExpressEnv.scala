package expressd

import java.lang.reflect

import scala.collection.mutable.HashMap

// Import InputFormat classes
import org.apache.hadoop.mapreduce.lib.input._

import spark.SparkContext

object ExpressEnv {

  def init() {
    if (sc == null) {
      sc = new SparkContext(
          if (System.getenv("MASTER") == null) "local" else System.getenv("MASTER"),
          "Express-Spark::" + java.net.InetAddress.getLocalHost.getHostName,
          System.getenv("EXPRESS_HOME"),
          Nil,
          executorEnvVars)
    }
  }

  def initWithSparkContext(userCreatedSparkContext: SparkContext) {
    sc = userCreatedSparkContext
  }

  var sc: SparkContext = _

  println("Initializing ExpressEnv")

  val executorEnvVars = new HashMap[String, String]
  executorEnvVars.put("SCALA_HOME", getEnv("SCALA_HOME"))
  executorEnvVars.put("SPARK_MEM", getEnv("SPARK_MEM"))
  executorEnvVars.put("SPARK_CLASSPATH", getEnv("SPARK_CLASSPATH"))
  executorEnvVars.put("HADOOP_HOME", getEnv("HADOOP_HOME"))
  executorEnvVars.put("JAVA_HOME", getEnv("JAVA_HOME"))
  executorEnvVars.put("MESOS_NATIVE_LIBRARY", getEnv("MESOS_NATIVE_LIBRARY"))

  val INPUT_FORMAT = {
    val formatClass = System.getProperty("express.inputFormat",
                                         "org.apache.hadoop.mapreduce.lib.input.TextInputFormat")
    Class.forName(formatClass)
  }

  val BIAS_WINDOW = System.getProperty("express.biasWindow", "8").toInt
  val BIAS_MATRIX_SIZE = BIAS_WINDOW * 2 + 1
  val MAX_FRAG_LENGTH = System.getProperty("express.maxFragLength", "800").toInt
  val MAX_READ_LENGTH = System.getProperty("express.maxReadLength", "200").toInt

  val BURN_IN_ITERATIONS = System.getProperty("express.burnInIterations", "20").toInt
  val UPDATE_INTERVAL = System.getProperty("express.updateInterval", "100").toInt
  val MAX_UPDATE_ITERATIONS = System.getProperty("express.maxUpdateIntervals", "1000").toInt
  val TAU_MIN_THRESHOLD = math.log(System.getProperty("express.tauMinThreshold", "1.0E-7").toDouble)
  val MAX_REL_CHANGE = math.log(System.getProperty("express.tauMinThreshold", "1.0E-2").toDouble)
  val OUTPUT_INTERVAL = System.getProperty("express.outputInterval", "-1").toInt
  val SHOULD_SERIALIZE_FRAGMENTS =
    System.getProperty("express.persist.serialized", "false").toBoolean

  val DEBUG = System.getProperty("express.debug", "false").toBoolean

  def getEnv(variable: String) =
    if (System.getenv(variable) == null) "" else System.getenv(variable)
}
