# Run create and run jar file for learn-spark.
Clear-Host
sbt package
spark-submit --class "Main" --master local `
    --jars "D:\Spark\sqljdbc_10.2\enu\mssql-jdbc-10.2.0.jre8.jar" `
           target\scala-2.12\learn-spark_2.12-1.0.jar `
    $($Env:SPARK_HOME+"\README.md")