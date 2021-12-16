package helloworld
import org.apache.spark.sql.SparkSession

object Main {
  def run(readmepath: String): Unit = {
    println("----------------------------------------------------------------------")
    println("  HelloWorld")
    println("----------------------------------------------------------------------")
    
    // Expect args(0) to be the path to SPARK_HOME.
    val logFile = readmepath
    println(s"Using $logFile as a data source")

    // A SparkSession is the main entry point to using spark.
    val spark = SparkSession.builder.appName("HelloWorld").getOrCreate()

    // Only use .config("spark.master", "local") with sbt. See readme for production environments.
    // val spark = SparkSession.builder.appName("Simple pplication").config("spark.master", "local").getOrCreate()
    
    // An implicit Encoder[Int] is needed to store Int instances in a Dataset.
    // Primitive types (Int, String, etc) and Product types (case classes) are supported by importing spark.implicits._
    // This is needed for creating a Dataset of [Int] in data.map(...)
    import spark.implicits._

    // Read in a file as a DataSet.
    val textFile = spark.read.textFile(logFile)

    // Cache in memory for fast access across the cluster.
    val data = textFile.cache()

    println(s"Number of lines: ${data.count()}")
    println(s"First line: ${data.first}")

    // Retrieve lines that fit a certain criteria.
    val linesSpark = data.filter(line => line.contains("Spark"))            // a Dataset containing lines that contains "Spark"
    val numLinesSpark = linesSpark.count()                                  // the number of lines containing "Spark"
    val numLinesA = data.filter(line => line.contains("a")).count()
    val numLinesB = data.filter(line => line.contains("b")).count()
    println("Number of lines with \"Spark\": " + s"$numLinesSpark")
    println("Number of lines with \"a\": " + s"$numLinesA")
    println("Number of lines with \"Spark\": " + s"$numLinesB")

    // map and flatMap are used to apply a function to every row in the Dataset.
    // The difference is that flatMap flattens any dimensions.
    val wordsMap = data.map(line => line.split(" "))                        // wordsMap is a Dataset[Array[String]]
    val wordsFlatMap = data.flatMap(line => line.split(" "))                // wordsFlatMap is a Dataset[String]

    // Find the number of words in the line with the most words.
    val numWordsPerLine = wordsMap.map(wordsArray => wordsArray.size)       // a Dataset[Int]
    val numWordsMax = numWordsPerLine.reduce(Math.max(_, _))
    println(s"Number of words in the line with most words: $numWordsMax")

    // Group the data by their content(words).
    val grouped = wordsFlatMap.groupByKey(identity)                         // a KeyValueGroupedDataset
    val wordCounts = grouped.count()                                        // a Dataset[Tuple2]

    // Convert the Dataset[Tuple2] to an Array[Tuple2]
    val wordCountsArray = wordCounts.collect()                              // an Array[Tuple2]
    println("Word Counts:")
    wordCountsArray.take(5).foreach({ case (a, b) => println(s"    $a: $b")})

    spark.stop()
    println()
  }
}