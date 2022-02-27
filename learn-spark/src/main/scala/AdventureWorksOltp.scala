package AdventureWorksOltp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.dense_rank
import org.apache.spark.sql.expressions.Window

object Main {
  // Rank products by their profit margin
  def productByMargin() = {
    val spark = SparkSession.builder().appName("Dataset Demo").getOrCreate()
    import spark.implicits._
    val product = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Production.Product.parquet")

    val colNames = List("Name","Margin","Rank")
    val wdw = Window.orderBy($"Margin".desc)
    val prodMargins = product
      // Create a new column called Margin from the difference of ListPrice and
      // StandardCost.
      .withColumn("Margin", $"ListPrice" - $"StandardCost")

      // Rank each product by descending Margin
      .withColumn("Rank", dense_rank().over(wdw))

      // Splat operator takes a collection and unpacks it into multiple functional arguments.
      // This is similar to Python's * operator, ie iterable*.
      .select(colNames.map(col):_*)

    println("----------------------------------------------------------------------")
    println("  DatasetDemo")
    println("----------------------------------------------------------------------")
    prodMargins.show()
  }

  def run() = {
    val spark = SparkSession.builder().appName("Dataset Demo").getOrCreate()
    val sod = spark.read.format("parquet").load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet")
    val soh = spark.read.format("parquet").load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderHeader.parquet")
    val product = spark.read.format("parquet").load(".\\parquet\\AdventureWorks-oltp\\Production.Product.parquet")
    val sp = spark.read.format("parquet").load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesPerson.parquet")
    val person = spark.read.format("parquet").load(".\\parquet\\AdventureWorks-oltp\\Person.Person.parquet")
  }
}