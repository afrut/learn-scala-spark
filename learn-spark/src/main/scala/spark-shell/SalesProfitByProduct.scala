// Compute net profit of each product across all sales
import scala.util.control.Breaks._

// Load data
val product = spark.read.format("parquet").
  load(".\\parquet\\AdventureWorks-oltp\\Production.Product.parquet").
  withColumnRenamed("Name", "ProductName")
val sod = spark.read.format("parquet").
  load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet")
product.createOrReplaceTempView("product")
sod.createOrReplaceTempView("sod")

// Aggregate data based on product type
val sodGrouped = sod.select("ProductID", "OrderQty", "LineTotal").
  groupBy("ProductID").
  agg(sum("OrderQty").as("OrderQty")
    , sum("LineTotal").as("LineTotal"))
val profit1 = product.
  join(sodGrouped, product("ProductID") === sod("ProductID"), "inner").
  withColumn("Profit", $"LineTotal" - ($"StandardCost" * $"OrderQty")).
  select("ProductName", "Profit", "LineTotal", "StandardCost", "OrderQty")
profit1.show()

// Same thing but with SQL
val query = 
  """select p.ProductName
  |     , sod.LineTotal - (p.StandardCost * sod.OrderQty) Profit
  |     , sod.LineTotal LineTotal
  |     , p.StandardCost
  |     , sod.OrderQty
  |from product p
  |join
  |(
  |  select ProductID
  |       , sum(LineTotal) LineTotal
  |       , sum(OrderQty) OrderQty
  |  from sod
  |  group by ProductID
  |) sod
  |on p.ProductID = sod.ProductID""".stripMargin
val profit2 = spark.sql(query)
profit2.show()