package AdventureWorksOltp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.dense_rank
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.lpad
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.expressions.Window

object Main {
  // Rank products by their profit margin
  def productByMargin() = {
    val spark = SparkSession.builder().appName("AdventureWorksOltp").getOrCreate()
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
    println("  Product Margins")
    println("----------------------------------------------------------------------")
    prodMargins.show()
    spark.stop()
  }

  // Find the top 3 products sold by each SalesPerson by quantity sold
  def productBySalesPersonByQuantity() = {
    val spark = SparkSession.builder().appName("AdventureWorksOltp").getOrCreate()
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

    println("----------------------------------------------------------------------")
    println("  Top 3 Products per Sales Person by Quantity")
    println("----------------------------------------------------------------------")
    topProducts.show(50)
    spark.stop()
  }

  // Find the top 3 products sold by each SalesPerson by revenue
  def productBySalesPersonByRevenue() = {
    val spark = SparkSession.builder().appName("AdventureWorksOltp").getOrCreate()
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
    println("----------------------------------------------------------------------")
    println("  Top 3 Products per Sales Person by Revenue")
    println("----------------------------------------------------------------------")
    topProducts.show(50)
    spark.stop()
  }

  // Find the top 3 products sold by each SalesPerson by profit
  def productBySalesPersonByProfit() = {
    val spark = SparkSession.builder().appName("AdventureWorksOltp").getOrCreate()
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
    println("----------------------------------------------------------------------")
    println("  Top 3 Products per Sales Person by Profit")
    println("----------------------------------------------------------------------")
    topProducts.show(50)
    spark.stop()
  }

  // Rank products by quantity sold per territory
  def productByQuantityPerTerritory() = {
    val spark = SparkSession.builder().appName("AdventureWorksOltp").getOrCreate()
    import spark.implicits._
    val sod = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet")
    val soh = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderHeader.parquet")
    val st = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesTerritory.parquet")
    val product = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Production.Product.parquet")

    val productsByTerritory = sod
      .join(soh, sod("SalesOrderID") === soh("SalesOrderID"), "inner")
      .join(st, soh("TerritoryID") === st("TerritoryID"))
      .join(product, sod("ProductID") === product("ProductID"))
      .groupBy(st("Name"),product("Name"))
      .agg(sum(sod("OrderQty")).as("OrderQtyTotal")
        ,sum(sod("LineTotal")).as("LineTotal2"))
      .select(
        st("Name").as("TerritoryName")
        ,product("Name").as("ProductName")
        ,$"OrderQtyTotal"
        ,$"LineTotal2"
      )
      .withColumnRenamed("LineTotal2", "LineTotal")
      .withColumn("Rank", dense_rank()
        .over(Window.partitionBy("TerritoryName").orderBy($"LineTotal".desc))
      )

    println("----------------------------------------------------------------------")
    println("  Products by Quantity per Territory")
    println("----------------------------------------------------------------------")
    productsByTerritory.show()
    spark.stop()
  }

  // Rank products by quantity sold
  def productRank(rankBy: String) = {
    val spark = SparkSession.builder().appName("AdventureWorksOltp").getOrCreate()
    import spark.implicits._
    val sod = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet")
    val product = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Production.Product.parquet")
    val rankByCol = rankBy match {
      case "Quantity" => Some(col("OrderQtyTotal"))
      case "Revenue" => Some(col("LineTotal"))
      case "Profit" => Some(col("Profit"))
      case _ => None
    }
    val productsByQuantity = rankByCol match {
      case Some(rankByCol) => sod
        .join(product, sod("ProductID") === product("ProductID"), "inner")
        .withColumn("Margin", product("ListPrice") - product("StandardCost"))
        .groupBy(product("ProductID"), product("Name").as("ProductName"))
        .agg(
          sum(sod("OrderQty")).as("OrderQtyTotal")
          ,sum(sod("LineTotal")).as("LineTotal2")
          ,sum(sod("OrderQty") * $"Margin").as("Profit")
        )
        .select("ProductName", "OrderQtyTotal", "LineTotal2", "Profit")
        .withColumnRenamed("LineTotal2", "LineTotal")
        .withColumn("Rank", 
        dense_rank()
          .over(Window.orderBy(rankByCol.desc))
        )
      case None => spark.emptyDataFrame
    }

    println("----------------------------------------------------------------------")
    println(s"  Products by ${rankBy}")
    println("----------------------------------------------------------------------")
    productsByQuantity.show()
    spark.stop()
  }

  // Rank products by reviews
  def productRating() = {
    val spark = SparkSession.builder().appName("AdventureWorksOltp").getOrCreate()
    import spark.implicits._
    val pr = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Production.ProductReview.parquet")
    val product = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Production.Product.parquet")
    val productRating = pr
      .join(product, pr("ProductID") === product("ProductID"), "inner")
      .groupBy($"Name").agg(avg($"Rating").as("RatingAvg"))
      .withColumn("Rank", dense_rank().over(Window.orderBy($"RatingAvg".desc)))
      .withColumnRenamed("Name", "ProductName")
      .select($"ProductName", $"RatingAvg".as("Rating"), $"Rank")
    println("----------------------------------------------------------------------")
    println(s"  Products Ranked by Average Review Rating")
    println("----------------------------------------------------------------------")
    productRating.show()
    spark.stop()
  }

  // Rank sales person by revenue or profit
  def salesPersonBy(rankBy: String) = {
    val spark = SparkSession.builder().appName("AdventureWorksOltp").getOrCreate()
    import spark.implicits._
    val soh = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderHeader.parquet")
    val sod = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet")
    val product = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Production.Product.parquet")
    val person = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Person.Person.parquet")

    val rankByCol = rankBy match {
      case "Revenue" => Some(col("Revenue"))
      case "Profit" => Some(col("Profit"))
      case _ => None
    }

    val salesPerson = rankByCol match {
      case Some(rankCol) =>
        soh
          .join(sod, soh("SalesOrderID") === sod("SalesOrderID"), "inner")
          .join(product, sod("ProductID") === product("ProductID"))
          .withColumn("Margin", product("ListPrice") - product("StandardCost").cast(DoubleType))
          .groupBy(soh("SalesPersonID"))
          .agg(
            sum(sod("LineTotal")).as("Revenue")
            ,sum($"Margin" * sod("OrderQty")).as("Profit")
          )
          .withColumn("Rank", lpad(dense_rank().over(Window.orderBy(rankCol.desc)), 3, "0"))
          .join(person, soh("SalesPersonID") === person("BusinessEntityID"), "left")
          .withColumn("SalesPersonName"
            ,when(soh("SalesPersonID").isNull, "ONLINE")
            .otherwise(concat($"LastName", lit(", "), $"FirstName")))
          .select($"SalesPersonName", rankCol, $"Rank")
      case None => spark.emptyDataFrame
    }
    println("----------------------------------------------------------------------")
    println(s"  Sales Person Ranked by $rankBy")
    println("----------------------------------------------------------------------")
    salesPerson.show()
    spark.stop()
  }

  // Rank territories by revenue or profit
  def territoryBy(rankBy: String, domestic: Boolean = false) = {
    val spark = SparkSession.builder().appName("AdventureWorksOltp").getOrCreate()
    import spark.implicits._
    val soh = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderHeader.parquet")
    val sod = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet")
    val st = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Sales.SalesTerritory.parquet")
    val product = spark.read.format("parquet")
      .load(".\\parquet\\AdventureWorks-oltp\\Production.Product.parquet")
    val rankByCol = rankBy match {
      case "Revenue" => Some(col("Revenue"))
      case "Profit" => Some(col("Profit"))
      case _ => None
    }
    val df = rankByCol match {
      case Some(rankCol) => {
        val ret = soh.join(sod, soh("SalesOrderID") === sod("SalesOrderID"))
          .join(product, sod("ProductID") === product("ProductID"))
          .withColumn("Margin", product("ListPrice") - product("StandardCost"))
          .withColumn("Profit", sod("OrderQty") * $"Margin")
          .groupBy(soh("TerritoryID"))
          .agg(
            sum(sod("LineTotal")).as("Revenue")
            ,sum($"Profit").as("Profit")
          )
          .join(st, soh("TerritoryID") === st("TerritoryID"))
          .withColumnRenamed("Name", "TerritoryName")
          .withColumn("Rank", dense_rank().over(Window.orderBy(rankCol.desc)))
          .select($"TerritoryName", rankCol, $"Rank")
        domestic match {
          case true => ret.filter($"TerritoryName".isin(
            Seq("Southwest","Northwest","Central","Southeast","Northeast"):_*))
          case false => ret
        }
      }
      case None => spark.emptyDataFrame
    }
    println("----------------------------------------------------------------------")
    println(s"  Territories Ranked by $rankBy")
    println("----------------------------------------------------------------------")
    df.show()
    spark.stop()
  }
}