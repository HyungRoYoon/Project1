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

  def problem1Question1(): Unit = {
    problemAnswers.getTotalNumberOfB1Consumers(spark)
  }

  def problem1Question2(): Unit = {
    problemAnswers.getTotalNumberOfB2Consumers(spark)
  }

  def problem1Question3(): Unit = {
    problemAnswers.createSinglePhysicalTab(spark)
  }

  def problem1Question4(): Unit = {
    problemAnswers.createMultiplePhysicalTab(spark)
  }

  //</editor-fold>

  //<editor-fold desc="Problem 2">

  def problem2Question1(): Unit = {
    problemAnswers.getMostConsumedBevB1(spark)
  }

  def problem2Question2(): Unit = {
    problemAnswers.getLeastConsumedBevB1(spark)
  }

  def problem2Question3(): Unit = {
    problemAnswers.getAveragedBevB1(spark)
  }

  //</editor-fold>

  //<editor-fold desc="Problem 3">

  def problem3Question1(): Unit = {
    problemAnswers.getAvailableBev(spark)
  }

  def problem3Question2(): Unit = {
    problemAnswers.getCommonAvailableBev(spark)
  }

  //</editor-fold>

  //<editor-fold desc="Problem 4">

  def problem4Question1(): Unit = {
    problemAnswers.createPartitionForScenario3(spark)
  }

  //</editor-fold>

  //<editor-fold desc="Problem 5">

  def problem5Question1(): Unit = {
    problemAnswers.alterTableProperties(spark)
  }

  def problem5Question2(): Unit = {
    problemAnswers.removeRowFromAnyScenario(spark)
  }

  //</editor-fold>

  //<editor-fold desc="Problem 6">

  def problem6Question1(): Unit = {

  }

  //</editor-fold>

}
