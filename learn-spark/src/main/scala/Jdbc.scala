// A file demonstrating the use of DataFrame (aka. Dataset[Row] in Scala) in Spark
package jdbc
import org.apache.spark.sql.SparkSession // For entry point.
import java.sql.DriverManager
import java.sql.Connection

object Main {
  def run(): Unit = {
    println("----------------------------------------------------------------------")
    println("  Jdbc")
    println("----------------------------------------------------------------------")
    val spark = SparkSession.builder().appName("Dataset Demo").getOrCreate() // Create SparkSession entry point.
    
    val tableQuery = """select s.name + '.' + t.name TableName
      |from sys.tables t
      |join sys.schemas s
      |on t.schema_id = s.schema_id
      |where s.name != 'dbo'""".stripMargin
    val dftn = spark.read.format("jdbc")
        .option("url",  "jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks;trustServerCertificate=true;integratedsecurity=true")
        .option("query", tableQuery)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()

    // collect() returns Array[Row]
    // tableNames: Array[String]
    val tableNames = dftn.collect().map(_.getString(0))

    tableNames.map((tableName) => {
      val df = (spark.read.format("jdbc")
        .option("url",  "jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks;trustServerCertificate=true;integratedsecurity=true")
        .option("dbtable", tableName)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load())
      df.write.format("parquet").mode("overwrite").save(s".\\parquet\\AdventureWorks-oltp\\${tableName}.parquet")
      // Another way to write parquet files:
      // df.write.mode("overwrite").parquet(s".\\parquet\\AdventureWorks-oltp\\${tableName}.parquet")
    })

    spark.stop()
    println()
  }
}