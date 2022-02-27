package AdventureWorksOltp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.dense_rank
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.sum
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

  // Find the top 3 products sold by each SalesPerson by quantity sold
  def productBySalesPersonByQuantity() = {
    val spark = SparkSession.builder().appName("Dataset Demo").getOrCreate()
    import spark.implicits._
    val sod = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet")
    val soh = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderHeader.parquet")
    val product = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Production.Product.parquet")
    val sp = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesPerson.parquet")
    val person = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Person.Person.parquet")

    // All sales
    val sales = sod
      .join(soh, sod("SalesOrderID") === soh("SalesOrderID"), "inner")
      .join(product, sod("ProductID") === product("ProductID"), "inner")
      .join(sp, soh("SalesPersonID") === sp("BusinessEntityID"), "left")
      .join(person, soh("SalesPersonID") === person("BusinessEntityID"), "left")
      .withColumn("SalesPersonName",
        when($"SalesPersonID".isNull, "ONLINE")
        .otherwise(concat(person("LastName"), lit(", "), person("FirstName"))))
      .select($"SalesPersonName"
        ,product("ProductID")
        ,product("Name").as("ProductName")
        ,sod("OrderQty"))

    // All sales grouped by SalesPersonName and ProductID and summing Orderqty
    val salesGrouped = sales.groupBy("SalesPersonName","ProductID", "ProductName")
      .agg(sum("OrderQty").as("OrderQty"))
      .select("SalesPersonName","ProductName","OrderQty")

    // For every combination of SalesPersonName and ProductName,
    // order by OrderQty and compute rank.
    val wdw = Window.partitionBy("SalesPersonName")
      .orderBy($"OrderQty".desc)
    val topProducts = salesGrouped.withColumn("Rank", dense_rank().over(wdw))
      .filter($"Rank" <= 3)
      .orderBy("SalesPersonName","Rank")
    topProducts.show(50)
  }

  // Find the top 3 products sold by each SalesPerson by revenue
  def productBySalesPersonByRevenue() = {
    val spark = SparkSession.builder().appName("Dataset Demo").getOrCreate()
    import spark.implicits._
    val sod = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet")
    val soh = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderHeader.parquet")
    val product = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Production.Product.parquet")
    val sp = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesPerson.parquet")
    val person = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Person.Person.parquet")

    // All sales
    val sales = sod
      .join(soh, sod("SalesOrderID") === soh("SalesOrderID"), "inner")
      .join(product, sod("ProductID") === product("ProductID"), "inner")
      .join(sp, soh("SalesPersonID") === sp("BusinessEntityID"), "left")
      .join(person, soh("SalesPersonID") === person("BusinessEntityID"), "left")
      .withColumn("SalesPersonName",
        when($"SalesPersonID".isNull, "ONLINE")
        .otherwise(concat(person("LastName"), lit(", "), person("FirstName"))))
      .select($"SalesPersonName"
        ,product("ProductID")
        ,product("Name").as("ProductName")
        ,sod("LineTotal"))

    // All sales grouped by SalesPersonName and ProductID and summing Orderqty
    val salesGrouped = sales.groupBy("SalesPersonName","ProductID", "ProductName")
      .agg(sum("LineTotal").as("LineTotal"))
      .select("SalesPersonName","ProductName","LineTotal")

    // For every combination of SalesPersonName and ProductName,
    // order by LineTotal and compute rank.
    val wdw = Window.partitionBy("SalesPersonName")
      .orderBy($"LineTotal".desc)
    val topProducts = salesGrouped.withColumn("Rank", dense_rank().over(wdw))
      .filter($"Rank" <= 3)
      .orderBy("SalesPersonName","Rank")
    topProducts.show(50)
  }

  // Find the top 3 products sold by each SalesPerson by profit
  def productBySalesPersonByProfit() = {
    val spark = SparkSession.builder().appName("Dataset Demo").getOrCreate()
    import spark.implicits._
    val sod = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet")
    val soh = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderHeader.parquet")
    val product = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Production.Product.parquet")
    val sp = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesPerson.parquet")
    val person = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Person.Person.parquet")

    // All sales
    val sales = sod
      .join(soh, sod("SalesOrderID") === soh("SalesOrderID"), "inner")
      .join(product, sod("ProductID") === product("ProductID"), "inner")
      .join(sp, soh("SalesPersonID") === sp("BusinessEntityID"), "left")
      .join(person, soh("SalesPersonID") === person("BusinessEntityID"), "left")
      .withColumn("SalesPersonName",
        when($"SalesPersonID".isNull, "ONLINE")
        .otherwise(concat(person("LastName"), lit(", "), person("FirstName"))))
      .withColumn("ProfitMargin", product("ListPrice") - product("StandardCost"))
      .withColumn("Profit", $"ProfitMargin" * sod("OrderQty"))
      .select($"SalesPersonName"
        ,product("ProductID")
        ,product("Name").as("ProductName")
        ,$"Profit")

    // All sales grouped by SalesPersonName and ProductID and summing Orderqty
    val salesGrouped = sales.groupBy("SalesPersonName","ProductID", "ProductName")
      .agg(sum("Profit").as("Profit"))
      .select("SalesPersonName","ProductName","Profit")

    // For every combination of SalesPersonName and ProductName,
    // order by Profit and compute rank.
    val wdw = Window.partitionBy("SalesPersonName")
      .orderBy($"Profit".desc)
    val topProducts = salesGrouped.withColumn("Rank", dense_rank().over(wdw))
      .filter($"Rank" <= 3)
      .orderBy("SalesPersonName","Rank")
    topProducts.show(50)
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