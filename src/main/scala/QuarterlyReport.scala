import scala.io.StdIn._

//UI related only
object QuarterlyReport {

  val databaseManager = new DatabaseManager

  def main(args: Array[String]): Unit = {

    MiscManager.purgePreviousTableData()
    MiscManager.WaitForSeconds(2000)
    Init()
    getUserChoiceCheck()
  }

  def Init(): Unit = {
    databaseManager.createDatabase()
  }

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
          |""".stripMargin)
      println("========================")
      chooseScenario(readLine("Please choose scenario: ").toInt)
      println(" ")
    }
    catch {
      case e: Exception => println("Please choose from existing choices. Current Exception(s):\n"+e.toString)
        println("========================")
    }
  }

  // choose scenario number 1~6
  def chooseScenario(x: Int) : Unit = x match {
    case 0 => quitProgram()
    case 1 => firstProblem()
    case 2 => secondProblem()
    case 3 => thirdProblem()
    case 4 => fourthProblem()
    case 5 => fifthProblem()
    case 6 => sixthProblem()
    case _ => reEnterNumber(x)
  }

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
    }
    case "p6" =>
      println(
        """
          |0: Go Back
          |1: Question for future query?
          |""".stripMargin)
      problemResult(readLine("Please enter question number: ").toInt)
      def problemResult(x: Int): Unit = x match {
      case 0 => goBack()
      case 1 => databaseManager.problemQuestions("p6q1")
    }
    case _ => println("Wrong input")

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

  def quitProgram(): Unit = {
    println(" ")
    databaseManager.closeSpark()
    println("Exiting interface...")
    System.exit(0)
  }


  def firstProblem(): Unit ={
    executeProblemSolution("p1")
  }

  def secondProblem(): Unit = {
    executeProblemSolution("p2")
  }

  def thirdProblem(): Unit = {
    executeProblemSolution("p3")
  }

  def fourthProblem(): Unit = {
    executeProblemSolution("p4")
  }

  def fifthProblem(): Unit = {
    executeProblemSolution("p5")
  }

  def sixthProblem(): Unit = {
    executeProblemSolution("p6")
  }


}
