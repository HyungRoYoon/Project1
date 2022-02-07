import org.apache.spark.sql._

case class Person(name: String, age: Long)

class DatabaseManager {
  System.setProperty("hadoop.home.dir", "C:\\winutils")
  case class beverages(name: String, branch: String)

  val problemAnswers = new ProblemAnswers
  val bevBranchTxt = "resources/bevBranch.txt"
  val bevCustomerCountTxt = "resources/bevCustomerCount.txt"

  val spark = SparkSession.builder()
    .appName("QuarterlyReport")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()

  println("created spark session")

  import spark.implicits._

  def createDatabase(): Unit =
  {
    //customize following sql
    spark.sql("create table if not exists bevBranch(beverage String,branch String) row format delimited fields terminated by ','");
    spark.sql("create table if not exists bevCustomerCount(beverage String, count Int) row format delimited fields terminated by ','");

    //**** once loaded, data is persistent unless manipulated ****
    spark.sql(s"LOAD DATA LOCAL INPATH '$bevBranchTxt' INTO TABLE bevBranch")
    spark.sql(s"LOAD DATA LOCAL INPATH '$bevCustomerCountTxt' INTO TABLE bevCustomerCount")

    val beverageTable = spark.sparkContext.textFile("resources/bevBranch.txt")
    val df = beverageTable.map(_.split(",")).map{case Array(a,b) => (a,b)}.toDF("Beverage", "Branch")

    //TODO: remove this when testing and functioning is confirmed
    df.show(100)
    spark.sql("SELECT * FROM bevBranch order by branch asc").show(100)
    spark.sql("SELECT * FROM bevCustomerCount").show(100)
  }

  def closeSpark(): Unit = {
    println("Closing spark connection...")
    spark.close
  }

  //pass spark to problemAnswers

  //<editor-fold desc="Problem 1">

  def problemQuestions(problemQuestionNumber: String): Unit = problemQuestionNumber match {
    case "p1q1" => problemAnswers.getTotalNumberOfB1Consumers(spark)
    case "p1q2" => problemAnswers.getTotalNumberOfB2Consumers(spark)
    case "p1q3" => problemAnswers.createMultiplePhysicalTab(spark)
    case "p2q1" => problemAnswers.getMostConsumedBevB1(spark)
    case "p2q2" => problemAnswers.getLeastConsumedBevB1(spark)
    case "p2q3" => problemAnswers.getAveragedBevB1(spark)
    case "p3q1" => problemAnswers.getAvailableBev(spark)
    case "p3q2" => problemAnswers.getCommonAvailableBev(spark)
    case "p4q1" => problemAnswers.createPartitionForScenario3(spark)
    case "p5q1" => problemAnswers.alterTableProperties(spark)
    case "p5q2" => problemAnswers.removeRowFromAnyScenario(spark)
    case "p6q1" => problemAnswers.futureQuery1(spark)
  }

  //</editor-fold>
}
