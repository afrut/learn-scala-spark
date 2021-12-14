// A simple application using spark to process a text file.
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]){
    // Expect args(0) to be the path to SPARK_HOME.
    val logFile = args(0) + "\\README.md"

    // A SparkSession is the main entry point to using spark.
    //val spark = SparkSession.builder.appName("Simple pplication").getOrCreate()

    // Only use .config("spark.master", "local") with sbt. See readme for production environments.
    val spark = SparkSession.builder.appName("Simple pplication").config("spark.master", "local").getOrCreate()

    // Read a file and cache in memory for fast access across the cluster.
    val logData = spark.read.textFile(logFile).cache()

    // Count number of lines that contain "a" and "b".
    val numLinesA = logData.filter(x => x.contains("a")).count()
    val numLinesB = logData.filter(x => x.contains("b")).count()
    println(s"numLinesA = $numLinesA, numLinesB = $numLinesB")

    spark.stop()
  }
}
