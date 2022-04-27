// Compute net profit of each product across all sales
import scala.util.control.Breaks._

// Load data
val product = spark.read.format("parquet").
  load(".\\parquet\\AdventureWorks-oltp\\Production.Product.parquet").
  withColumnRenamed("Name", "ProductName")
val soh = spark.read.format("parquet").
  load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderHeader.parquet")
val sod = spark.read.format("parquet").
  load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet")
val person = spark.read.format("parquet").
  load(".\\parquet\\AdventureWorks-oltp\\Person.Person.parquet")
product.createOrReplaceTempView("product")
sod.createOrReplaceTempView("sod")
soh.createOrReplaceTempView("soh")
person.createOrReplaceTempView("person")

// Retrieve SalesPersonName
val sp = soh.
  join(person, soh("SalesPersonID") === person("BusinessEntityID"), "left").
  withColumn("SalesPersonName", when(soh("SalesPersonID").isNull, "ONLINE")
    .otherwise(concat(person("LastName"), lit(", "), person("FirstName")))).
  select("SalesPersonName", "SalesOrderID")

// Aggregate data based on sales person and product
val sodGrouped = sod.
  join(sp, sod("SalesOrderID") === sp("SalesOrderID"), "inner").
  groupBy(sp("SalesPersonName"), sod("ProductID")).
  agg(sum("OrderQty").as("OrderQty")
    , sum("LineTotal").as("LineTotal")).
  select("SalesPersonName", "ProductID", "OrderQty", "LineTotal")

// Compute profit
val profit1 = product.
  join(sodGrouped, product("ProductID") === sodGrouped("ProductID"), "inner").
  withColumn("Profit", $"LineTotal" - ($"StandardCost" * $"OrderQty")).
  select("SalesPersonName", "ProductName", "Profit", "LineTotal", "StandardCost", "OrderQty")
profit1.show()

// --------------------------------------------------------------------------------
// Retrieve SalesPersonName
spark.sql(
  """select soh.SalesOrderID SalesOrderID
  |       , case when p.BusinessEntityID is null then 'ONLINE'
  |           else concat(p.LastName, ', ', p.FirstName)
  |         end SalesPersonName
  |from soh
  |left join person p
  |on soh.SalesPersonID = p.BusinessEntityID
  """.stripMargin).
  createOrReplaceTempView("sp")

// Aggregate data based on sales person and product
spark.sql(
  """select sp.SalesPersonName SalesPersonName
  |       , sod.ProductID ProductID
  |       , sum(LineTotal) LineTotal
  |       , sum(OrderQty) OrderQty
  |from sod
  |join sp
  |on sod.SalesOrderID = sp.SalesOrderID
  |group by sp.SalesPersonName
  |       , sod.ProductID
  """.stripMargin).
  createOrReplaceTempView("sodGrouped")

// Compute profit
spark.sql(
  """select sg.SalesPersonName SalesPersonName
  |       , p.ProductName ProductName
  |       , sg.LineTotal - (p.StandardCost * sg.OrderQty) Profit
  |       , sg.LineTotal LineTotal
  |       , p.StandardCost StandardCost
  |       , sg.OrderQty OrderQty
  |from sodGrouped sg
  |join product p
  |on sg.ProductID = p.ProductID
  """.stripMargin).
  createOrReplaceTempView("profit")

spark.sql("select * from profit").show()