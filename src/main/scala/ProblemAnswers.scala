import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class ProblemAnswers {

  //<editor-fold desc="Comment">

  //See all joined data
  //sparkSession.sql("SELECT branch, beverage, t2.count FROM bevBranch t1 " +
  //"left join bevCustomerCount t2 using (beverage) " +
  //"group by branch, beverage, t2.count order by branch").show(100)

  //</editor-fold>

  //<editor-fold desc="Assign Variables">

  val p1 = 1
  val p2 = 2
  val p3 = 3
  val p4 = 4
  val p5 = 5
  val p6 = 6

  //</editor-fold>

  //<editor-fold desc="Problem 1">

  def getTotalNumberOfB1Consumers(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    //use join
    //println("without subquery")
    sparkSession.sql("SELECT first(branch) as Branch, first(beverage) as Beverage, sum(t2.count) as Total_Customer_Count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) where branch = 'Branch1'" +
      "group by branch order by branch asc").show(1)
//    println("with subquery")
//    sparkSession.sql("SELECT first(branch) as Branch, first(beverage) as Beverage, sum(t2.count) as Total_Customer_Count FROM bevBranch t1 " +
//      "left join bevCustomerCount t2 using (beverage) where branch in (select branch from bevBranch where branch = 'Branch1')" +
//      "group by branch order by branch asc").show(1)
    QuarterlyReport.problemNumber(p1)
  }

  def getTotalNumberOfB2Consumers(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT first(branch) as Branch, first(beverage) as Beverage, sum(t2.count) as Total_Customer_Count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) where branch = 'Branch2'" +
      "group by branch order by branch asc").show(1)
    QuarterlyReport.problemNumber(p1)
  }

  //use one of following two
  def createMultiplePhysicalTab(sparkSession: SparkSession): Unit =
  {
    sparkSession.sql("create table if not exists Branch1SalesInfo SELECT branch as Branch, beverage as Beverage, t2.count as Total_Customer_Count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) where branch = 'Branch1'" +
      "group by branch, beverage, t2.count order by branch asc")
    sparkSession.sql("create table if not exists Branch2SalesInfo SELECT branch as Branch, beverage as Beverage, t2.count as Total_Customer_Count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) where branch = 'Branch2'" +
      "group by branch, beverage, t2.count order by branch asc")
    sparkSession.sql("create table if not exists Branch3SalesInfo SELECT branch as Branch, beverage as Beverage, t2.count as Total_Customer_Count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) where branch = 'Branch3'" +
      "group by branch, beverage, t2.count order by branch asc")
    sparkSession.sql("show tables").show(10)
    MiscManager.WaitForSeconds(500)
    QuarterlyReport.problemNumber(p1)
  }

  //</editor-fold>

  //<editor-fold desc="Problem 2">

  def getMostConsumedBevB1(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT beverage as Beverage, sum(t2.count) as Total_Customer_Count FROM bevBranch t1 " +
    "left join bevCustomerCount t2 using (beverage) where branch = 'Branch1'" +
    "group by beverage order by Total_Customer_Count desc limit 1").show(1)
    QuarterlyReport.problemNumber(p2)
  }

  def getLeastConsumedBevB2(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT beverage as Beverage, sum(t2.count) as Total_Customer_Count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) where branch = 'Branch2'" +
      "group by beverage order by Total_Customer_Count asc limit 1").show(1)
    QuarterlyReport.problemNumber(p2)
  }

  def getAveragedBevB2(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT beverage as Beverage, round(avg(t2.count),0) as Total_Customer_Count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) where branch = 'Branch2'" +
      "group by beverage order by Total_Customer_Count asc limit 1").show(1)
    QuarterlyReport.problemNumber(p2)
  }

  //</editor-fold>

  //<editor-fold desc="Problem 3">

  def getAvailableBev(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT branch as Branch, beverage as Beverage from bevBranch where branch in ('Branch1', 'Branch8', 'Branch10') " +
      "group by branch, beverage order by branch").show(100)
    QuarterlyReport.problemNumber(p3)
  }

  def getCommonAvailableBev(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT beverage as Beverage FROM bevBranch t1 " +
    "left join bevBranch t2 using (beverage) where t1.branch in ('Branch4', 'Branch7') " +
    "group by beverage order by beverage asc").show(100)
    QuarterlyReport.problemNumber(p3)
  }

  //</editor-fold>

  //<editor-fold desc="Problem 4">

  def createPartitionForScenario3(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    sparkSession.sql("create table if not exists partitionTable(beverage String) partitioned by (branch String)")
    sparkSession.sql("insert overwrite table partitionTable partition(branch) select beverage, branch from bevBranch")

    sparkSession.sql("select beverage, branch from partitionTable group by branch, beverage order by branch").show(100)
    sparkSession.sql("describe formatted partitionTable").show()

    println("below is for view on Scenario 3")
    sparkSession.sql("create view if not exists Scenario3View as select beverage from partitionTable group by beverage")
    sparkSession.sql("select * from Scenario3View group by beverage").show(100)
    println("=========================")
    QuarterlyReport.problemNumber(p4)
  }

  //</editor-fold>

  //<editor-fold desc="Problem 5">

  def alterTableProperties(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("alter table bevBranch add columns (note string, comment string)")
    sparkSession.sql("describe formatted bevBranch").show()
    QuarterlyReport.problemNumber(p5)
  }

  def removeRowFromAnyScenario(sparkSession: SparkSession): Unit = {
    MiscManager.WaitForSeconds(500)
    //use dataframe where or filter to remove row
    import sparkSession.implicits._
    val beverageTable = sparkSession.sparkContext.textFile("resources/bevCustomerCount.txt")
    val df = beverageTable.map(_.split(",")).map{case Array(a,b) => (a,b)}.toDF("Beverage", "Count")
    println("BEFORE removing row where value is 21: ")
    df.show(100)
    println("AFTER removing row where value is 21: ")
    df.where("count != 21").show(100)
    QuarterlyReport.problemNumber(p5)
  }

  //</editor-fold>

  //<editor-fold desc="Problem 6">

  def top3BestWorst(sparkSession: SparkSession): Unit = {
    //go easy for first one...top 3 best/worst selling beverage across all branch
    MiscManager.WaitForSeconds(500)
    println("Top 3 best selling beverages across all branches are: ")

//    sparkSession.sql("SELECT beverage as Beverage, sum(t2.count) as Total_Customer_Count FROM bevBranch t1 " +
//      "left join bevCustomerCount t2 using (beverage) where branch = 'Branch1'" +
//      "group by beverage order by Total_Customer_Count desc limit 1").show(1)
//
    sparkSession.sql("SELECT beverage as Beverage, sum(t2.count) as Total_Customer_Count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) " +
      "group by beverage order by Total_Customer_Count desc limit 3").show

    println("Top 3 worst selling beverages across all branches are: ")
    sparkSession.sql("SELECT beverage as Beverage, sum(t2.count) as Total_Customer_Count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) " +
      "group by beverage order by Total_Customer_Count asc limit 3").show
    QuarterlyReport.problemNumber(p6)
  }

  def totalConsumersPerBranch(sparkSession: SparkSession): Unit = {
    //total consumers per branch
    MiscManager.WaitForSeconds(500)
    sparkSession.sql("SELECT first(branch) as Branch, sum(t2.count) as Total_Customer_Count FROM bevBranch t1 " +
      "left join bevCustomerCount t2 using (beverage) where branch in ('Branch1', 'Branch2', 'Branch3', 'Branch4', 'Branch5', 'Branch6', 'Branch7', 'Branch8', 'Branch9')" +
      "group by branch order by branch asc").show(10)
    QuarterlyReport.problemNumber(p6)
  }

  //</editor-fold>
}
