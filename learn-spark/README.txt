Run .\env.ps1 to set environment variables first.
Note the modifications to the build configuration of this project in build.sbt.
To run this project, and pass the path in SPARK_HOME to the application:
    - sbt "~run $($Env:SPARK_HOME)"
Create a jar file containing the spark application in target\scala-2.12\:
    - sbt package
To run the program in the jar file:
    - spark-submit --class "Main" --master local target\scala-2.12\learn-spark_2.12-1.0.jar $($Env:SPARK_HOME+"\README.md")
To include the jdbc jar file that enables connection to an SQL server:
    - spark-submit --class "Main" --master local --jars "D:\Spark\sqljdbc_10.2\enu\mssql-jdbc-10.2.0.jre8.jar" target\scala-2.12\learn-spark_2.12-1.0.jar $($Env:SPARK_HOME+"\README.md")
SparkSession.master must be defined. To do so, either:
    - Put the line "spark.master local" in $Env:SPARK_HOME\conf\spark-defaults.conf
    - Or pass --master local to spark-submit
To disable all INFO messages printing to console, in $Env:SPARK_HOME\conf\log4j.properties, ensure that the following line is present:
    - log4j.rootCategory=ERROR, console
To disable ERROR messages coming from ShutdownHookManager, in $Env:SPARK_HOME\conf\log4j.properties, ensure that the following line is present:
    - log4j.logger.org.apache.spark.util.ShutdownHookManager=OFF
To use in spark-shell:
    - run spark-shell through console
    - to add a library, spark-shell --jars "D:\Spark\sqljdbc_10.2\enu\mssql-jdbc-10.2.0.jre8.jar"
    - :load ".\path\to\file.scala"
    - call the method within the object Main.main()
    - Spark UI for spark shell is at localhost:4040