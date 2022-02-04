import scala.reflect.io.Directory
import java.io.File

object MiscManager {

  val derbyLogPath = new Directory(new File("D:\\IdeaProjects\\repo\\project1\\derby.log"))
  val sparkWarehousePath = new Directory(new File("D:\\IdeaProjects\\repo\\project1\\spark-warehouse"))
  val metastoreDBPath = new Directory(new File("D:\\IdeaProjects\\repo\\project1\\metastore_db"))
  val outputPath = ""

  def purgePreviousTableData(): Unit = {

    println("Removing previous data...")
    if (!derbyLogPath.deleteRecursively() && !sparkWarehousePath.deleteRecursively() && !metastoreDBPath.deleteRecursively()) {
      println("Trying to delete files but they don't exist. ")
    }
    else {
      derbyLogPath.deleteRecursively()
      sparkWarehousePath.deleteRecursively()
      metastoreDBPath.deleteRecursively()
    }
  }

  def WaitForSeconds(seconds: Int): Unit = {
    Thread.sleep(seconds)
  }
}
