import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.nio.file.Paths

import models.{BufferedOutputStreamExtension, OperationsResultFile}
import models.operations.Operation

import scala.io.Source
import scala.util.Try

/**
 * This object does the chain execution actions
 */
object ChainExecutor {

  val csvRegex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"

  /**
    * This method executes the given operations on a given csv file
    *
    * @param csvPath - the path of the csv file
    * @param workingDirectoryPath - the current working directory path
    * @param operations - the seq of operations that we want to execute
    *
    * @return - the path to the folder where the result file exists
    */
  def executeChainOfOperations(csvPath: String,
                               workingDirectoryPath: String,
                               operations: Seq[Operation]): String = {

    implicit val workingDirectory: String = workingDirectoryPath

    // first we need to prepare our working directory to the mission
    val inputFile = preProcess(csvPath)

    // execute each of the operations
    val outputFile = operations.foldLeft(inputFile) {
      (currentInput, currentOperation) =>
        currentOperation.getResult(currentInput)
    }

    //
    afterProcess(outputFile)

  }

  /**
    * This method writes each of the columns of the given csv to a different file
    * and adding to each it's metadata
    *
    * @param csvPath - the path of the csv
    * @param workingDirectory - the working directory path
    * @return
    */
  private def preProcess(csvPath: String)(
      implicit workingDirectory: String): OperationsResultFile = {
    val file = OperationsResultFile.createNewFile()

    val source = Source.fromFile(csvPath)

    val linesStream = source.getLines().toStream

    var columnNames = Seq[String]()
    var fileColumnsStreams = Map[Int, BufferedOutputStreamExtension]()

    linesStream.zipWithIndex.foreach {
      case (line, index) => {
        // in case we are at the first line we just want to init all the needed files
        // and add the column name to each file
        if (index == 0) {
          columnNames = line.split(csvRegex)
          fileColumnsStreams = columnNames.zipWithIndex.map {
            case (columnName, index) => {
              val outputStream = new BufferedOutputStreamExtension(
                new FileOutputStream(
                  Paths.get(file.path, index.toString).toFile))
              outputStream.writeLine(columnName)
              index -> outputStream
            }
          }.toMap
          // in case we are at the second line we want to determine if this column is numerical,
          // write it to file and write the current value
        } else if (index == 1) {
          line.split(csvRegex).zipWithIndex.foreach {
            case (value, columnIndex) => {
              val isColumnNumerical = Try(value.toDouble).isSuccess
              fileColumnsStreams(columnIndex).writeLine(
                isColumnNumerical.toString)
              fileColumnsStreams(columnIndex).writeLine(value)
            }
          }
          // in all other cases we just write the value to the file
        } else {
          line.split(csvRegex).zipWithIndex.foreach {
            case (value, columnIndex) => {
              fileColumnsStreams(columnIndex).writeLine(value)
            }
          }
        }
      }
    }

    source.close()

    fileColumnsStreams.foreach(_._2.close())

    file
  }

  /**
    * This method changes each result file extension to be .csv and then return the folder path
    *
    * @param file - the final directory
    *
    * @return the final directory path
    */
  private def afterProcess(file: OperationsResultFile): String = {
    Paths.get(file.path).toFile.listFiles().foreach { file =>
      file.renameTo(new File(file.getAbsolutePath + ".csv"))
    }

    file.path
  }
}
