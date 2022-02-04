import org.apache.spark.sql._

class ProblemAnswers {
//bevBranch(beverage String,branch String), bevCustomerCount(beverage String, count Int)

  //<editor-fold desc="Problem 1">

  def getTotalNumberOfB1Consumers(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    //use join
    sparkSession.sql("SELECT first(branch) as Branch, first(beverage) as Beverage, sum(t2.count) as Total_Customer_Count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) where branch = 'Branch1'" +
      "group by branch order by branch asc").show(1)
    QuarterlyReport.firstProblem()
  }

  def getTotalNumberOfB2Consumers(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT first(branch) as Branch, first(beverage) as Beverage, sum(t2.count) as Total_Customer_Count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) where branch = 'Branch2'" +
      "group by branch order by branch asc").show(1)
    QuarterlyReport.firstProblem()
  }

  //use one of following two
  def createSinglePhysicalTab(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT first(branch) as Branch, first(beverage) as Beverage, sum(t2.count) as Total_Customer_Count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) " +
      "group by branch order by branch asc").show(100)
    QuarterlyReport.firstProblem()
  }

  def createMultiplePhysicalTab(sparkSession: SparkSession): Unit =
  {
    println("Type 2: Creating multiple physical tables.")
    sparkSession.sql("SELECT * FROM bevCustomerCount").show(100)
    MiscManager.WaitForSeconds(500)
    QuarterlyReport.firstProblem()
  }

  //</editor-fold>

  //<editor-fold desc="Problem 2">

  def getMostConsumedBevB1(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT * FROM bevCustomerCount").show(100)
    QuarterlyReport.secondProblem()
  }

  def getLeastConsumedBevB1(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT * FROM bevCustomerCount").show(100)
    QuarterlyReport.secondProblem()
  }

  def getAveragedBevB1(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT * FROM bevCustomerCount").show(100)
    QuarterlyReport.secondProblem()
  }

  //</editor-fold>

  //<editor-fold desc="Problem 3">

  def getAvailableBev(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT * FROM bevCustomerCount").show(100)
    QuarterlyReport.thirdProblem()
  }

  def getCommonAvailableBev(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT * FROM bevCustomerCount").show(100)
    QuarterlyReport.thirdProblem()
  }

  //</editor-fold>

  //<editor-fold desc="Problem 4">

  def createPartitionForScenario3(sparkSession: SparkSession): Unit = {
    println("Create a partition, view for the scenario 3")
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT * FROM bevCustomerCount").show(100)
    QuarterlyReport.fourthProblem()
  }

  //</editor-fold>

  //<editor-fold desc="Problem 5">

  def alterTableProperties(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT * FROM bevCustomerCount").show(100)
    QuarterlyReport.fifthProblem()
  }

  def removeRowFromAnyScenario(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT * FROM bevCustomerCount").show(100)
    QuarterlyReport.fifthProblem()
  }

  //</editor-fold>

  //<editor-fold desc="Problem 6">

  def futureQuery1(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT * FROM bevCustomerCount").show(100)
    QuarterlyReport.sixthProblem()
  }

  //</editor-fold>


}
