// A file demonstrating the use of Dataframe (aka. Dataset[Row] in Scala) in Spark
package datasetdemo
import org.apache.spark.sql.SparkSession // For entry point.
import org.apache.spark.sql.functions.col // Creates a new Column with the given column name.
import org.apache.spark.sql.Row

object Main {
  def printClass(x: Any) = println(x.getClass)

  def run(jsonfilepath: String): Unit = {
    println("----------------------------------------------------------------------")
    println("  DatasetDemo")
    println("----------------------------------------------------------------------")
    println(s"Using $jsonfilepath as data source.")
    println()

    val spark = SparkSession.builder().appName("Dataset Demo").getOrCreate() // Create SparkSession entry point.
    import spark.implicits._ // For using $-notation with Dataframe.select()
    
    // ----------------------------------------
    //   Creation
    // ----------------------------------------
    val df = spark.read.json(jsonfilepath)  // Create a Dataframe from a json file.

    // ----------------------------------------
    //   Selection
    // ----------------------------------------
    df.select("class") // Select a single column.
    df.select("class", "petal_length") // Select multiple columns.
    df.select(col("class")) // Select columns with col() function.
    df.select($"class", ($"petal_length" + 0.73)) // Select columns with $-notation and expressions.
    df.select(col("class").alias("target")) // Select column and alias.
    df.filter($"petal_length" < 1.3) // Select columns that match a criteria.
    df.first() // First row.
    df.foreach(x => {}) // Apply a function to every row.
    df.foreachPartition((x: Iterator[Row]) => x.foreach(y => {})) // Apply a function for every atomic chunk of data sotred on a node in a cluster. Used for costly operations that involve eg. creating database connections.
    df.head() // First row.
    df.head(5) // First 5 rows.
    // TODO: reduce

    // ----------------------------------------
    //   Conversion
    // ----------------------------------------
    df.collect() // Return Dataframe as Array[Row]
    df.collectAsList() // Return a Java ArrayList

    // ----------------------------------------
    //   Inspection
    // ----------------------------------------
    df.show(5) // Show top 5 rows. Default 20 with no argument passed in.
    df.printSchema() // Show column structure.
    df.describe() // Returns a Dataframe of basic statistics.
    df.summary() // Same as describe but with percentiles

    // ----------------------------------------
    //   Aggregation
    // ----------------------------------------
    df.count() // Number of rows in the Dataframe.
    df.groupBy("class").count() // Count the number of rows belonging to each distinct value of column "class".

    spark.stop()
    println()
  }
}