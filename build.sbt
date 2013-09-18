import AssemblyKeys._

name := "express-D"

version := "0.1"

organization := "math.mcb.berkeley.edu"

scalaVersion := "2.9.3"

libraryDependencies += "org.clapper" % "argot_2.9.2" % "0.4"

libraryDependencies += "net.sf.jopt-simple" % "jopt-simple" % "4.5"

libraryDependencies += "org.scalatest" % "scalatest_2.9.2" % "1.8" % "test"

libraryDependencies += "com.google.guava" % "guava" % "14.0.1"

unmanagedJars in Compile <++= baseDirectory map { base =>
  val pathToSpark = System.getenv("SPARK_HOME")
  val finder: PathFinder = (file(pathToSpark)) ** "*.jar"
  print ("finder: " + finder)
  finder.get
}

assemblySettings

test in assembly := {}

jarName in assembly := "express-D-assembly.jar"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => 
  {
    case PathList("META-INF", xs @ _*) =>
      (xs.map(_.toLowerCase)) match {
        case ("manifest.mf" :: Nil) => MergeStrategy.discard
        // Note(harvey): this to get Shark perf test assembly working.
        case ("license" :: Nil) => MergeStrategy.discard
        case ps @ (x :: xs) if ps.last.endsWith(".sf") => MergeStrategy.discard
        case _ => MergeStrategy.first
      }
    case PathList("reference.conf", xs @ _*) => MergeStrategy.concat
    case PathList("application.conf", xs @ _*) => MergeStrategy.concat
    case _ => MergeStrategy.first
  }
}
