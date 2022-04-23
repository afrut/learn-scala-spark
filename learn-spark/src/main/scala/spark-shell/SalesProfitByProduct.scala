// Compute net profit of each product across all sales

// Load data
val product = spark.read.format("parquet").
  load(".\\parquet\\AdventureWorks-oltp\\Production.Product.parquet").
  withColumnRenamed("Name", "ProductName")
val sod = spark.read.format("parquet").
  load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet")

// Aggregate data based on product type
val sodGrouped = sod.select("ProductID", "OrderQty", "LineTotal").
  groupBy("ProductID").
  agg(sum("OrderQty").as("OrderQty")
    , sum("LineTotal").as("LineTotal"))
val profit = product.
  join(sodGrouped, product("ProductID") === sod("ProductID"), "inner").
  withColumn("Profit", $"LineTotal" - ($"StandardCost" * $"OrderQty")).
  select("ProductName", "Profit", "LineTotal", "StandardCost", "OrderQty")
profit.show()