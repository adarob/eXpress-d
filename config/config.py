"""Configuration options for running Express-D"""

import time

from config_utils import FlagSet
from config_utils import JavaOptionSet
from config_utils import OptionSet

# Flags that are required to compile eXpress using bin/build-and-run:
# - SPARK_HOME
# - SPARK_GIT_REPO
# - EXPRESS_D_HOME
#
# Flags required to run eXpress using bin/build-and-run or bin/run:
# - SPARK_CLUSER_URL
# - EXPRESS_RUNTIME_LOCAL_OPTS. The only two absolutely required are below, but the program might
#   not run as desired unless the other properties are set (e.g., specify 'should-use-bias').
#   - OptionSet("hits-file-path", [ "/path/to/hits.pb" ])
#   - OptionSet("targets-file-path", [ "/path/to/targets.pb" ])

# --------------------------------------------------------------------------------------------------
# Spark + Express-D Build Configuration Options
# --------------------------------------------------------------------------------------------------

# Required. Location of Spark sources.
SPARK_HOME = "/opt/mapr/spark/spark-1.4.1"
SPARK_CONF_DIR = SPARK_HOME + "/conf"

# If true, don't build Spark. This assumes that SPARK_HOME has been assembled using
# 'sbt/sbt assembly'.
SPARK_SKIP_PREP = True

# Required. Only applicable is SPARK_SKIP_BUILD is false. The git repository used to clone copies of
# Spark to path/to/express-D/spark.
SPARK_GIT_REPO = "git://github.com/mesos/spark.git -b branch-0.7"

# Required. Location of the Express-D root directory.
EXPRESS_D_HOME = "/home/hadoop/src/express-d"

# If true, don't build Express-D. Assumes that EXPRESS_D_HOME has a target directory that contains
# the assembly jar.
EXPRESS_D_SKIP_PREP = False

TIMINGS_OUTPUT_FILE = "express-D-run-%s" % time.strftime("%Y-%m-%d_%H-%M-%S")

# --------------------------------------------------------------------------------------------------
# General Configuration Options
# --------------------------------------------------------------------------------------------------
SCALA_CMD = "scala"

# This default setting assumes we are running on the Spark EC2 AMI. Users will probably want
# to change this to SPARK_CLUSTER_URL = "local" for running locally.
# SPARK_CLUSTER_URL = open("/root/spark-ec2/cluster-url", 'r').readline().strip()
SPARK_CLUSTER_URL = "local"

# --------------------------------------------------------------------------------------------------
# Spark + Express-D Runtime Options/Properties
#
# Note: these are actually Java options that will be passed to the SCALA_CMD call. Calling them
#       Java options are a bit confusing...
# --------------------------------------------------------------------------------------------------

# Global options specific to Spark.
SPARK_RUNTIME_OPTS = [
    # Fraction of JVM memory used for caching RDDs.
    JavaOptionSet("spark.storage.memoryFraction", [0.66]),
    JavaOptionSet("spark.serializer", ["spark.JavaSerializer"]),
    # How much memory to give the Spark process running on each slave.
    JavaOptionSet("spark.executor.memory", ["4g"]),
]

# Global options specific to Express-D.
EXPRESS_RUNTIME_OPTS = [
    JavaOptionSet("express.persist.serialize", ["false"]),
    JavaOptionSet("express.outputInterval", [5000]),
    JavaOptionSet("express.targetParallelism", [20]),
]

ALL_JAVA_OPTS = SPARK_RUNTIME_OPTS + EXPRESS_RUNTIME_OPTS

# Options local to each Express-D run.
EXPRESS_RUNTIME_LOCAL_OPTS = [
    OptionSet("hits-file-path", [""]),
    OptionSet("targets-file-path", [""]),
    OptionSet("should-use-bias", ["true"]),
    OptionSet("num-iterations", ["1000"]),
    OptionSet("should-cache", ["true"]),
    # Optional.
    OptionSet("num-partitions-for-alignments", ["-1"]),
    OptionSet("debug-output", ["false"]),
]

# A tuple of (short_name, test_cmd, scale_factor, java_opt_sets, opt_sets)
EXPRESS_RUN = [("Express-D", "expressd.ExpressRunner", 1.0, ALL_JAVA_OPTS, EXPRESS_RUNTIME_LOCAL_OPTS)]
