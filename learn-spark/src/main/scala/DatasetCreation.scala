// A file demonstrating the use of DataFrame (aka. Dataset[Row] in Scala) in Spark
package DatasetCreation
import org.apache.spark.sql.SparkSession // For entry point.

// Type of every row in the json file.
case class RowInstance(
  sepal_length: Double
  ,sepal_width:Double
  ,petal_length:Double
  ,petal_width:Double
  ,target: String
)

object Main {
  def printClass(x: Any) = println(x.getClass)

  def run(): Unit = {
    println("----------------------------------------------------------------------")
    println("  DatasetCreation")
    println("----------------------------------------------------------------------")
    val filepathJson = ".\\src\\main\\resources\\iris.json"
    val filepathParquet = ".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet"

    val spark = SparkSession.builder().appName("DatasetDataCreation").getOrCreate() // Create SparkSession entry point.
    import spark.implicits._ // For encoders whe using .as[RowInstance] and for .toDF()
    
    val dfJson = spark.read.json(filepathJson) // Create a DataFrame from a json file.
    val dfPq = spark.read.format("parquet").load(filepathParquet) // Read data for a DataFrame from a parquet file.
    val df = dfJson.as[RowInstance] // Cast types of each column as defined by case class.
    
    // Create data from Scala data types.
    val id = Seq(1, 2, 3, 4)
    val name = Seq("foo", "bar", "baz", "qaz")
    val df2 = id.zip(name).toDF()
    val df3 = id.zip(name).toDF("id", "name")

    // Create an empty DataFrame.
    val dfEmpty = spark.emptyDataFrame

    println(s"dfJson number of rows: ${dfJson.count}")
    println(dfJson.dtypes)
    println(s"dfPq number of rows: ${dfPq.count}")
    println(s"df number of rows: ${df.count}")
    println(s"df2 number of rows: ${df2.count}")
    println(s"df3 number of rows: ${df3.count}")
    println(s"dfEmpty number of rows: ${dfEmpty.count}")

    spark.stop()
    println()
  }
}