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

    val df = (spark.read.format("jdbc")
    .option("url",  "jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks;trustServerCertificate=true;integratedsecurity=true")
    .option("dbtable", "Person.Person")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load())
    println(df.count())

    spark.stop()
    println()
  }
}