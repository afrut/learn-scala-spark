// A file demonstrating different ways to create DataFrmaes (aka. Dataset[Row] in Scala) in Spark
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
    val filepathJson = ".\\src\\main\\resources\\iris.json"

    val spark = SparkSession.builder().appName("Dataset Demo").getOrCreate() // Create SparkSession entry point.
    import spark.implicits._ // For using $-notation with DataFrame.select(), encoders, and DataFrame.*

    val dfRaw = spark.read.json(filepathJson) // Create a DataFrame from a json file.
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
    df.select("target").distinct().show() // Get the distinct values of the column "target".
    df.filter($"petal_length" < 1.3) // Select columns that match a criteria.
    df.first() // First row.
    df.foreach(x => {}) // Apply a function to every row.
    df.foreachPartition((x: Iterator[RowInstance]) => x.foreach(y => {})) // Apply a function for every atomic chunk of data sotred on a node in a cluster. Used for costly operations that involve eg. creating database connections.
    df.head() // First row.
    df.head(5) // First 5 rows.
    df.limit(5) // Create DataFrame of first 5 rows.
    df.tail(5) // Last 5 rows.
    df.take(5) // First 5 rows.
    df.takeAsList(5) // First 5 rows as List
    df.toLocalIterator() // Return an Iterator[]
    // TODO: checkpoint
    // TODO: createGlobalTempView
    // TODO: createOrReplaceGlobalTempView
    // TODO: createOrReplaceTempView
    // TODO: createTempView
    // TODO: hint
    // TODO: isLocal
    // TODO: javaRDD
    // TODO: localCheckpoint(eager: Boolean)
    // TODO: localCheckpoint()
    // TODO: persist
    // TODO: rdd
    // TODO: storageLevel
    // TODO: unpersist

    // ----------------------------------------
    //   Conversion
    // ----------------------------------------
    df.collect() // Return DataFrame as Array[Row]
    df.collectAsList() // Return a Java ArrayList

    // ----------------------------------------
    //   Inspection
    // ----------------------------------------
    df.columns // Returns Array[String] for columnames of DataFrame.
    df.describe() // Returns a DataFrame of basic statistics.
    df.dtypes // Returns Array[String, String] for column names and data types
    df.inputFiles // Array[String] that lists files that make up this DataFrame.
    df.isEmpty // Returns if DataFrame has any rows or not.
    df.printSchema() // Show column structure.
    df.printSchema(3) // Show column structure up to level 3.
    df.schema // Returns a StructType (which is a collection of StructFields) which repersents, the columns and data types of the DataFrame
    df.show(5) // Show top 5 rows. Default 20 with no argument passed in.
    df.summary() // Same as describe but with percentiles

    // ----------------------------------------
    //   Aggregation
    // ----------------------------------------
    df.count() // Number of rows in the DataFrame.
    df.groupBy("target").count() // Count the number of rows belonging to each distinct value of column "target".
    df.reduce((a, b) => if(a.sepal_length > b.sepal_length) a else b) // Reduce the DataFrame to get the row with the greatest sepal_length.

    // ----------------------------------------
    //   Physical/Logical Plan
    // ----------------------------------------
    df.rdd.toDebugString // Description of the RDD and its recursive dependencies
    df.explain(true) // show parsed, analyzed, optimized and physical plans
    df.explain(mode = "simple") // show physical plan
    df.explain(mode = "extended") // show parsed, analyzed, optimized and physical plans
    df.explain(mode = "codegen") // shows java code to be executed
    df.explain(mode = "cost") // show optimized logical plan and physical plan
    df.explain(mode = "formatted") // show physical plan and details at every step
    
    spark.stop()
    println()
  }
}