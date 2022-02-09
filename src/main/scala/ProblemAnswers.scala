import org.apache.spark.sql._

class ProblemAnswers {
//bevBranch(beverage String,branch String), bevCustomerCount(beverage String, count Int)

  //see all joined data
  //sparkSession.sql("SELECT branch, beverage, t2.count FROM bevBranch t1 " +
  //"left join bevCustomerCount t2 using (beverage) " +
  //"group by branch, beverage, t2.count order by branch").show(100)

  //<editor-fold desc="Problem 1">

  def getTotalNumberOfB1Consumers(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    //use join
    sparkSession.sql("SELECT first(branch) as Branch, first(beverage) as Beverage, sum(t2.count) as Total_Customer_Count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) where branch = 'Branch1'" +
      "group by branch order by branch asc").show(1)
    sparkSession.sql("show tables")
    QuarterlyReport.firstProblem()
  }

  def getTotalNumberOfB2Consumers(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT first(branch) as Branch, first(beverage) as Beverage, sum(t2.count) as Total_Customer_Count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) where branch = 'Branch2'" +
      "group by branch order by branch asc").show(1)
    sparkSession.sql("show tables")
    QuarterlyReport.firstProblem()
  }

  //use one of following two
  def createMultiplePhysicalTab(sparkSession: SparkSession): Unit =
  {
    sparkSession.sql("create table if not exists Branch1SalesInfo SELECT branch, beverage, t2.count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) where branch = 'Branch1'" +
      "group by branch, beverage, t2.count order by branch asc").show(100)
    sparkSession.sql("create table if not exists Branch2SalesInfo SELECT branch, beverage, t2.count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) where branch = 'Branch2'" +
      "group by branch, beverage, t2.count order by branch asc").show(100)
    sparkSession.sql("create table if not exists Branch3SalesInfo SELECT branch, beverage, t2.count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) where branch = 'Branch3'" +
      "group by branch, beverage, t2.count order by branch asc").show(100)
    sparkSession.sql("show tables").show(10)
    MiscManager.WaitForSeconds(500)
    QuarterlyReport.firstProblem()
  }

  //</editor-fold>

  //<editor-fold desc="Problem 2">

  def getMostConsumedBevB1(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT beverage, sum(t2.count) as Total_Customer_Count FROM bevBranch t1 " +
    "left join bevCustomerCount t2 using (beverage) " +
    "group by beverage order by Total_Customer_Count desc limit 1").show(1)
//    sparkSession.sql("select beverage, sum(count) as Total_Customer_Count from bevCustomerCount " +
//      "group by beverage order by Total_Customer_Count desc limit 1").show(1)
    QuarterlyReport.secondProblem()
  }

  def getLeastConsumedBevB1(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT beverage, sum(t2.count) as Total_Customer_Count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) " +
      "group by beverage order by Total_Customer_Count asc limit 1").show(1)
//    sparkSession.sql("select beverage, sum(count) as Total_Customer_Count from bevCustomerCount " +
//      "group by beverage order by Total_Customer_Count asc limit 1").show(1)
    QuarterlyReport.secondProblem()
  }

  def getAveragedBevB1(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT beverage, round(avg(t2.count),0) as Total_Customer_Count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) " +
      "group by beverage order by Total_Customer_Count asc limit 1").show(1)
//    sparkSession.sql("select beverage, round(avg(count),0) as Total_Customer_Count from bevCustomerCount " +
//      "group by beverage order by Total_Customer_Count desc limit 1").show(1)
    QuarterlyReport.secondProblem()
  }

  //</editor-fold>

  //<editor-fold desc="Problem 3">

  def getAvailableBev(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT branch, beverage from bevBranch where branch in ('Branch1', 'Branch8', 'Branch10') group by branch, beverage order by branch").show(100)
    //println(sparkSession.sql("SELECT branch, beverage from bevBranch where branch in ('Branch1', 'Branch8','Branch10') group by branch, beverage order by branch").count().toString())
    QuarterlyReport.thirdProblem()
  }

  def getCommonAvailableBev(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT beverage FROM bevBranch t1 " +
    "left join bevBranch t2 using (beverage) where t1.branch in ('Branch4', 'Branch7') " +
    "group by beverage order by beverage asc").show(100)
    QuarterlyReport.thirdProblem()
  }

  //</editor-fold>

  //<editor-fold desc="Problem 4">

  def createPartitionForScenario3(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    sparkSession.sql("create table if not exists partitionTable(beverage String) partitioned by (branch String)")
    sparkSession.sql("insert overwrite table partitionTable partition(branch) select beverage, branch from bevBranch")
    //    spark.sql("INSERT OVERWRITE TABLE Partitioned_abc PARTITION(branches) SELECT bev,branch from all_branch")

    //sparkSession.sql("select * from partitionTable").show(100)
    sparkSession.sql("select beverage, branch from partitionTable group by branch, beverage order by branch").show(100)
    sparkSession.sql("describe formatted partitionTable").show()

    println("below is for view on Scenario 3")
    sparkSession.sql("create view if not exists Scenario3View as select beverage from partitionTable group by beverage")
    sparkSession.sql("select * from Scenario3View group by beverage").show(100)
    println("=========================")
    QuarterlyReport.fourthProblem()
  }

  //</editor-fold>

  //<editor-fold desc="Problem 5">

  def alterTableProperties(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    //sparkSession.sql("alter table bevBranch add if not exists 'note', 'comment'").show(100)
    sparkSession.sql("alter table bevBranch add columns (note string, comment string)")
    sparkSession.sql("describe formatted bevBranch").show()
    QuarterlyReport.fifthProblem()
  }

  def removeRowFromAnyScenario(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    //use dataframe where or filter to remove row
    import sparkSession.implicits._
    val beverageTable = sparkSession.sparkContext.textFile("resources/bevCustomerCount.txt")
    val df = beverageTable.map(_.split(",")).map{case Array(a,b) => (a,b)}.toDF("Beverage", "Count")
    df.where("count != 21").show(100)
    QuarterlyReport.fifthProblem()
  }

  //</editor-fold>

  //<editor-fold desc="Problem 6">

  def futureQuery1(sparkSession: SparkSession): Unit = {
    //go easy for first one...top 3 best/worst selling beverage across all branch
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT * FROM bevCustomerCount").show(100)
    QuarterlyReport.sixthProblem()
  }

  def futureQuery2(sparkSession: SparkSession): Unit = {
    //total consumers per branch
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT * FROM bevCustomerCount").show(100)
    QuarterlyReport.sixthProblem()
  }

  def futureQuery3(sparkSession: SparkSession): Unit = {
    //
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT * FROM bevCustomerCount").show(100)
    QuarterlyReport.sixthProblem()
  }

  //</editor-fold>
}
