import org.apache.spark.sql._


object TestObject {
//  def main(args: Array[String]): Unit = {
//    // create a spark session
//    // for Windows
//    System.setProperty("hadoop.home.dir", "C:\\winutils")
//
//    val spark = SparkSession
//      .builder
//      .appName("hello hive")
//      .config("spark.master", "local")
//      .enableHiveSupport()
//      .getOrCreate()
//    println("created spark session")
//    val sampleSeq = Seq((1, "spark"), (2, "Big Data"))
//
//    val df = spark.createDataFrame(sampleSeq).toDF("Course id", "course name")
//    df.show()
//    df.write.format("csv").save("sampleSeq")
//
//  }
case class Person(name: String, age: Long)
  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession.builder()
      .appName("HiveTest5")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
//
//    spark.sparkContext.setLogLevel("ERROR")
//    println("created spark session")
//    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
//    //spark.sql("CREATE TABLE IF NOT EXISTS src(key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ STORED AS TEXTFILE")
//    //spark.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' INTO TABLE src")
//    //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT,value STRING) USING hive")
//
//    spark.sql("create table if not exists newone1(id Int,name String) row format delimited fields terminated by ','");
//    spark.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' INTO TABLE newone1")
//    spark.sql("SELECT * FROM newone1").show()

  }
}
