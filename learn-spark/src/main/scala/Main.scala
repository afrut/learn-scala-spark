object Main {
  def main(args: Array[String]){
    val readmepath = args(0)
    // helloworld.Main.run(readmepath)
    // DatasetCreation.Main.run()
    // datasetdemo.Main.run()
    // jdbc.Main.run()
    // AdventureWorksOltp.Main.productByMargin()
    // AdventureWorksOltp.Main.productBySalesPersonByQuantity()
    // AdventureWorksOltp.Main.productBySalesPersonByRevenue()
    AdventureWorksOltp.Main.productBySalesPersonByProfit()
  }
}