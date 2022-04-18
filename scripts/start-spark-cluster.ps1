# spark-class org.apache.spark.deploy.master.Master
# spark-class org.apache.spark.deploy.worker.Worker spark://192.168.0.157:7077

# Start the master
$scriptblk = {
    $Host.UI.RawUI.WindowTitle='Spark Master';
    Set-Location $Env:SPARK_HOME\bin
    spark-class org.apache.spark.deploy.master.Master
}
Start-Process pwsh -ArgumentList "-NoExit", "-Command",$scriptblk

# Start workers
for($i = 0; $i -lt 3; $i++)
{
    $scriptblk = {
        $Host.UI.RawUI.WindowTitle='Spark Worker';
        Set-Location $Env:SPARK_HOME\bin
        spark-class org.apache.spark.deploy.worker.Worker spark://192.168.0.157:7077
    }
    Start-Process pwsh -ArgumentList "-NoExit", "-Command",$scriptblk
}