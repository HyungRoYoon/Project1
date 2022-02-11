import scala.io.StdIn._

//<editor-fold desc="Author">

//QuarterlyReport
//Hyung Ro Yoon
//2022/02/11

//</editor-fold>

//UI related only
object QuarterlyReport {

  //<editor-fold desc="Assign Variables">

  val databaseManager = new DatabaseManager
  val excelManager = new ExcelManager

  //</editor-fold>

  //<editor-fold desc="Main">

  def main(args: Array[String]): Unit = {

    MiscManager.purgePreviousTableData()
    MiscManager.WaitForSeconds(2000)
    Init()
    getUserChoiceCheck()
  }

  //</editor-fold>

  //<editor-fold desc="Init">

  def Init(): Unit = {
    databaseManager.createDatabase()
  }

  //</editor-fold>

  //<editor-fold desc="User Choices">

  def getUserChoiceCheck(): Unit = {
    try {
      print("========================")
      print(
        """
          |0: Quit Program
          |1: First Scenario
          |2: Second Scenario
          |3: Third Scenario
          |4: Fourth Scenario
          |5: Fifth Scenario
          |6: Sixth Scenario
          |7: MS Excel Spreadsheet Menu
          |""".stripMargin)
      println("========================")
      chooseScenario(readLine("Please choose scenario: ").toInt)
      println(" ")
    }
    catch {
      case e: Exception => println("Please choose from existing choices. Current Exception(s):\n"+e.printStackTrace())
        println("========================")
        getUserChoiceCheck()
    }
  }

  // choose scenario number 1~6
  def chooseScenario(x: Int) : Unit = x match {
    case 0 => quitProgram()
    case 1 => problemNumber(x)
    case 2 => problemNumber(x)
    case 3 => problemNumber(x)
    case 4 => problemNumber(x)
    case 5 => problemNumber(x)
    case 6 => problemNumber(x)
    case 7 => excelManager.excelMainMenu()
    case _ => reEnterNumber(x)
  }

  //</editor-fold>

  //<editor-fold desc="Execute problem solutions">

  //choose / execute problem solution
  def executeProblemSolution(problemNumber: String): Unit = problemNumber match {
    case "p1" =>
    println(
      """
        |0: Go Back
        |1: What is the total number of consumers for Branch1?
        |2: What is the total number of consumers for Branch2?
        |3: Type 1: Creating multiple physical tables
        |""".stripMargin)
      problemResult(readLine("Please enter question number: ").toInt)
      def problemResult(x: Int): Unit = x match {
      case 0 => goBack()
      case 1 => databaseManager.problemQuestions("p1q1")
      case 2 => databaseManager.problemQuestions("p1q2")
      case 3 => databaseManager.problemQuestions("p1q3")
      case _ => reEnterNumber(x)
    }
    case "p2" =>
      println(
      """
        |0: Go Back
        |1: What is the most consumed beverage on Branch1?
        |2: What is the least consumed beverage on Branch2?
        |3: What is the average consumed beverage of Branch2?
        |""".stripMargin)
      problemResult(readLine("Please enter question number: ").toInt)
      def problemResult(x: Int): Unit = x match {
      case 0 => goBack()
      case 1 => databaseManager.problemQuestions("p2q1")
      case 2 => databaseManager.problemQuestions("p2q2")
      case 3 => databaseManager.problemQuestions("p2q3")
      case _ => reEnterNumber(x)
    }
    case "p3" =>
      println(
        """
          |0: Go Back
          |1: What are the beverages available on Branch 1, Branch 8, and Branch 10?
          |2: What are the common beverages available in Branch4, Branch7?
          |""".stripMargin)
      problemResult(readLine("Please enter question number: ").toInt)
      def problemResult(x: Int): Unit = x match {
      case 0 => goBack()
      case 1 => databaseManager.problemQuestions("p3q1")
      case 2 => databaseManager.problemQuestions("p3q2")
      case _ => reEnterNumber(x)
    }
    case "p4" =>
      println(
        """
          |0: Go Back
          |1: Create a partition, view for the scenario 3
          |""".stripMargin)
      problemResult(readLine("Please enter question number: ").toInt)
      def problemResult(x: Int): Unit = x match {
      case 0 => goBack()
      case 1 => databaseManager.problemQuestions("p4q1")
      case _ => reEnterNumber(x)
    }
    case "p5" =>
      println(
        """
          |0: Go Back
          |1: Alter the table properties to add 'note', 'comment'
          |2: Remove a row from the any Scenario
          |""".stripMargin)
      problemResult(readLine("Please enter question number: ").toInt)
      def problemResult(x: Int): Unit = x match {
      case 0 => goBack()
      case 1 => databaseManager.problemQuestions("p5q1")
      case 2 => databaseManager.problemQuestions("p5q2")
      case _ => reEnterNumber(x)
    }
    case "p6" =>
      println(
        """
          |0: Go Back
          |1: Top 3 Best/Worst Selling Beverage
          |2: Total Consumers per Branch
          |""".stripMargin)
      problemResult(readLine("Please enter question number: ").toInt)
      def problemResult(x: Int): Unit = x match {
      case 0 => goBack()
      case 1 => databaseManager.problemQuestions("p6q1")
      case 2 => databaseManager.problemQuestions("p6q2")
      case _ => reEnterNumber(x)
    }
    case _ => println("Wrong input: Correct input required at code level")
  }

  def reEnterNumber(x: Int) : Unit = {
    if (x != 0) println("Please select one of given numbers.")
    getUserChoiceCheck()
  }

  def reEnterNumber(problemNumber: String) : Unit = {
    if (problemNumber != 0) println("Please select one of given numbers.")
    getUserChoiceCheck()
  }

  def goBack(): Unit = {
    getUserChoiceCheck()
  }

  def problemNumber(x: Int): Unit = x match {
    case 1 => executeProblemSolution("p1")
    case 2 => executeProblemSolution("p2")
    case 3 => executeProblemSolution("p3")
    case 4 => executeProblemSolution("p4")
    case 5 => executeProblemSolution("p5")
    case 6 => executeProblemSolution("p6")
  }

  //</editor-fold>

  //<editor-fold desc="Quit">

  def quitProgram(): Unit = {
    println(" ")
    databaseManager.closeSpark()
    //MiscManager.purgePreviousTableData()
    println("Exiting interface...")
    System.exit(0)
  }

  //</editor-fold>
}
