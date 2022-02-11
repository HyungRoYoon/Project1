import org.apache.poi.ss.usermodel.{DataFormatter, WorkbookFactory}

import java.io.{File, FileInputStream, FileOutputStream}
import scala.io.StdIn._
import org.apache.spark.sql._
import com.github.mrpowers.spark.daria.sql.DariaWriters
import org.apache.poi.hssf.usermodel.HSSFRow
import org.apache.poi.xssf.usermodel.{XSSFSheet, XSSFWorkbook}

import java.awt.Desktop

class ExcelManager {

  var fileName = " "
  var csvLocation = " "

  // Automatically convert Java collections to Scala equivalents
  import scala.collection.JavaConversions._

  def exportData(df: DataFrame, sparkSession: SparkSession): Unit = {
    fileName = readLine("Please enter file name to save with: ")+".csv"
    println("Creating Excel file...")
    DariaWriters.writeSingleFile(df = df, format = "csv", sc = sparkSession.sparkContext,
      tmpFolder = MiscManager.outputPath.toString(), filename = MiscManager.outputPath+"\\"+fileName)
    csvLocation = MiscManager.outputPath+"\\"+fileName
    excelMainMenu()
  }

  def readCSV(): Unit = {
    val file = new FileInputStream(new File(csvLocation))
    val df = QuarterlyReport.databaseManager.returnSparkSession()
      .read.option("inferSchema", "true").csv(s"$csvLocation")
      .toDF("Branch", "Beverage", "Total_Customer_Count").show(100)
    excelMainMenu()
  }
//
//  def writeCSV(): Unit = {
//    val file = new FileInputStream(new File(csvLocation))
//    val workbook = new XSSFWorkbook(file)
//    val sheet = workbook.getSheetAt(0)
//    var sheetRow = sheet.getRow(0)
//    if (sheetRow == null) {
//      sheetRow == sheet.createRow(0)
//    }
//
//    var cell = sheetRow.getCell(0)
//    if (cell == null) {
//      cell = sheetRow.createCell(0)
//    }
//    cell.setCellValue("Branch")
//
//    file.close()
//    excelMainMenu()
//  }
//
//  def createGraph(): Unit = {
//
//    excelMainMenu()
//  }


  def excelMainMenu(): Unit = {
    try {
      print("========================")
      print(
        """
          |0: Go Back
          |1: Save to Excel file
          |2: Read Excel file
          |3: Open Excel file
          |""".stripMargin)
      println("========================")
      getUserExcelChoice(readLine("Please choose from menu: ").toInt)
      println(" ")
    }
    catch {
      case e: Exception =>
        println("Please choose from existing choices. Current Exception(s):\n"+e.printStackTrace())
        println("========================")
        QuarterlyReport.chooseScenario(7)
    }
  }

  def getUserExcelChoice(x: Int): Unit = x match {
    case 0 => QuarterlyReport.goBack()
    case 1 => saveToCSV()
    case 2 => readCSV()
    case 3 => launchExcel()
    case _ => QuarterlyReport.reEnterNumber(x)
  }

  def saveToCSV(): Unit = {
    QuarterlyReport.databaseManager.exportQueryResult()
  }

  def launchExcel(): Unit = {
    val excelFile = new File(csvLocation)
    Desktop.getDesktop().open(excelFile)
    excelMainMenu()
  }
}
