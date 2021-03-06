import org.apache.spark.sql._

class DatabaseManager {
  System.setProperty("hadoop.home.dir", "C:\\winutils")
  case class beverages(name: String, branch: String)

  //<editor-fold desc="Assign Variables">

  val problemAnswers = new ProblemAnswers
  val excelManager = new ExcelManager
  val bevBranchTxt = "resources/bevBranch.txt"
  val bevCustomerCountTxt = "resources/bevCustomerCount.txt"

  val spark = SparkSession.builder()
    .appName("QuarterlyReport")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()

  //</editor-fold>

  println("created spark session")

  import spark.implicits._

  //<editor-fold desc="Create Database">

  def createDatabase(): Unit =
  {
    //customize following sql
    spark.sql("create table if not exists bevBranch(beverage String,branch String) row format delimited fields terminated by ','");
    spark.sql("create table if not exists bevCustomerCount(beverage String, count Int) row format delimited fields terminated by ','");

    //**** once loaded, data is persistent unless manipulated ****
    spark.sql(s"LOAD DATA LOCAL INPATH '$bevBranchTxt' INTO TABLE bevBranch")
    spark.sql(s"LOAD DATA LOCAL INPATH '$bevCustomerCountTxt' INTO TABLE bevCustomerCount")
  }

  //</editor-fold>

  //<editor-fold desc="Close Spark">

  def closeSpark(): Unit = {
    println("Closing spark connection...")
    spark.close
  }

  //</editor-fold>

  //<editor-fold desc="Problem 1">

  //pass spark to problemAnswers
  def problemQuestions(problemQuestionNumber: String): Unit = problemQuestionNumber match {
    case "p1q1" => problemAnswers.getTotalNumberOfB1Consumers(spark)
    case "p1q2" => problemAnswers.getTotalNumberOfB2Consumers(spark)
    case "p1q3" => problemAnswers.createMultiplePhysicalTab(spark)
    case "p2q1" => problemAnswers.getMostConsumedBevB1(spark)
    case "p2q2" => problemAnswers.getLeastConsumedBevB2(spark)
    case "p2q3" => problemAnswers.getAveragedBevB2(spark)
    case "p3q1" => problemAnswers.getAvailableBev(spark)
    case "p3q2" => problemAnswers.getCommonAvailableBev(spark)
    case "p4q1" => problemAnswers.createPartitionForScenario3(spark)
    case "p5q1" => problemAnswers.alterTableProperties(spark)
    case "p5q2" => problemAnswers.removeRowFromAnyScenario(spark)
    case "p6q1" => problemAnswers.top3BestWorst(spark)
    case "p6q2" => problemAnswers.totalConsumersPerBranch(spark)
  }

  //</editor-fold>

  //<editor-fold desc="Export Query Result">

  def exportQueryResult(): Unit = {
    val df = spark.sql("SELECT branch, beverage, t2.count FROM bevBranch t1 " +
      "inner join bevCustomerCount t2 using (beverage) " +
      "group by branch, beverage, t2.count order by branch")
    excelManager.exportData(df, spark)
  }

  def returnSparkSession(): SparkSession = {
    spark
  }
  //</editor-fold>

}
