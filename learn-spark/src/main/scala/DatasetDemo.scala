// A file demonstrating the use of Dataframe (aka. Dataset[Row] in Scala) in Spark
package datasetdemo
import org.apache.spark.sql.SparkSession // For entry point.
import org.apache.spark.sql.functions.col // Creates a new Column with the given column name.
import org.apache.spark.sql.Row

// Type of every row in the json file.
case class RowInstance(sepal_length: Double, sepal_width:Double, petal_length:Double, petal_width:Double, target: String)

object Main {
  def printClass(x: Any) = println(x.getClass)

  def run(): Unit = {
    println("----------------------------------------------------------------------")
    println("  DatasetDemo")
    println("----------------------------------------------------------------------")
    val filepath = ".\\src\\main\\resources\\iris.json"
    println(s"Using $filepath as data source.")
    println()

    val spark = SparkSession.builder().appName("Dataset Demo").getOrCreate() // Create SparkSession entry point.
    import spark.implicits._ // For using $-notation with Dataframe.select()
    
    // ----------------------------------------
    //   Creation
    // ----------------------------------------
    val dfRaw = spark.read.json(filepath) // Create a Dataframe from a json file.
    val df = dfRaw.as[RowInstance] // Cast types of each column as defined by case class.
    df.cache() // Most spark operations are lazy, usually evaluating when an action is invoked. Cache upon first loading to prevent repeated loading.

    // ----------------------------------------
    //   Selection
    // ----------------------------------------
    df.select("target") // Select a single column.
    df.select("target", "petal_length") // Select multiple columns.
    df.select(col("target")) // Select columns with col() function.
    df.select($"target", ($"petal_length" + 0.73)) // Select columns with $-notation and expressions.
    df.select(col("target").alias("target")) // Select column and alias.
    df.filter($"petal_length" < 1.3) // Select columns that match a criteria.
    df.first() // First row.
    df.foreach(x => {}) // Apply a function to every row.
    df.foreachPartition((x: Iterator[RowInstance]) => x.foreach(y => {})) // Apply a function for every atomic chunk of data sotred on a node in a cluster. Used for costly operations that involve eg. creating database connections.
    df.head() // First row.
    df.head(5) // First 5 rows.
    df.tail(5) // Last 5 rows.
    df.take(5) // First 5 rows.
    df.takeAsList(5) // First 5 rows as List
    df.toLocalIterator() // Return an Iterator[]
    // TODO: checkpoint
    // TODO: demonstration of cache
    // TODO: createGlobalTempView
    // TODO: createOrReplaceGlobalTempView
    // TODO: createOrReplaceTempView
    // TODO: createTempView
    // TODO: explain
    // TODO: hint

    // ----------------------------------------
    //   Conversion
    // ----------------------------------------
    df.collect() // Return Dataframe as Array[Row]
    df.collectAsList() // Return a Java ArrayList

    // ----------------------------------------
    //   Inspection
    // ----------------------------------------
    df.columns // Returns Array[String] for columnames of DataFrame.
    df.describe() // Returns a Dataframe of basic statistics.
    df.dtypes // Returns Array[String, String] for column names and data types
    println(s"input files: ${df.inputFiles}")
    df.printSchema() // Show column structure.
    df.show(5) // Show top 5 rows. Default 20 with no argument passed in.
    df.summary() // Same as describe but with percentiles

    // ----------------------------------------
    //   Aggregation
    // ----------------------------------------
    df.count() // Number of rows in the Dataframe.
    df.groupBy("target").count() // Count the number of rows belonging to each distinct value of column "target".
    df.reduce((a, b) => if(a.sepal_length > b.sepal_length) a else b) // Reduce the Dataframe to get the row with the greatest sepal_length.

    spark.stop()
    println()
  }
}