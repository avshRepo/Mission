package models

import java.io.FileOutputStream
import java.nio.file.{Files, Paths}
import java.util.UUID

import scala.io.{BufferedSource, Source}

/**
 * This is the file that we work on for doing the chaining
 *
 * @param path - the path of the file
 */
case class OperationsResultFile(path: String) {

  /**
   * This method copies the column to the output file
   *
   * @param columnNumber - the column number
   * @param outputFile - the output file
   * @param useColumnNumberForOutput - in case it is true it means we use filter
   *                                   and therefore use the the same column number,
   *                                   otherwise we always write at the default column
   * @param indexes - the indexes that we want to take
   */
  def copyColumn(columnNumber: Int, outputFile: OperationsResultFile, useColumnNumberForOutput: Boolean, indexes: Stream[Int] = Stream.empty[Int]): Unit = {

    // determine at which column we write
    val outputColumnNumber = if (useColumnNumberForOutput) columnNumber else OperationsResultFile.defaultColumn

    // create the new output file
    val outputColumnFilePath = Paths.get(outputFile.path, outputColumnNumber.toString)

    // get columns stream
    val source = getColumnAsStream(columnNumber)

    // create output stream in order to write to the file
    val target = new BufferedOutputStreamExtension(new FileOutputStream(outputColumnFilePath.toFile))

    source.getLines().toStream.zipWithIndex.foreach {
      case (line, index) => {
        // in case we use filter and index != 0, 1 this is because index 0 and 1 are for metadata
        if (useColumnNumberForOutput && index != 0 && index != 1) {
          // check if index in filtered indexes
          if (indexes.contains(index)) {
            target.writeLine(line)
          }
        } else {
          target.writeLine(line)
        }
      }
    }

    source.close()
    target.close()
  }

  /**
   * This method get column file as stream
   *
   * @param columnNumber - the requested column
   *
   * @return - stream of the column file
   */
  def getColumnAsStream(columnNumber: Int): BufferedSource = {
    val inputColumnFilePath = Paths.get(path, columnNumber.toString)

    Source.fromFile(inputColumnFilePath.toUri)
  }

  /**
   * This method get the column metadata from the column file
   *
   * @param columnNumber - the column number
   *
   * @return - the column metadata
   */
  def getColumnMetadata(columnNumber: Int): ColumnMetadata = {
    val source = getColumnAsStream(columnNumber)

    val metadataLines = source.getLines().toStream.take(OperationsResultFile.numberOfMetadataLines)

    val columnName = metadataLines.head
    val isColumnNumerical = metadataLines.last.toBoolean

    source.close()

    ColumnMetadata(columnName, isColumnNumerical)
  }

  /**
   * This method creates new file and add its metadata
   *
   * @param columnNumber - the column number
   * @param columnName - the column name
   * @param isNumerical - is this column is numerical or not
   *
   * @return - the output stream
   */
  def writeNewColumn(columnNumber: Int, columnName: String, isNumerical: Boolean): BufferedOutputStreamExtension = {
    val newFile = new BufferedOutputStreamExtension(new FileOutputStream(Paths.get(path, columnNumber.toString).toFile))

    newFile.writeLine(columnName)
    newFile.writeLine(isNumerical.toString)

    newFile
  }

  /**
   * This method get the indexes of the rows that match the filter condition
   *
   * @param columnNumber - the column number
   * @param valueToMatch - the value we want to check if exists
   * @param isNumerical - is this coulmn numerical
   *
   * @return - stream of indexes
   */
  def getIndexsThatMatch(columnNumber: Int, valueToMatch: String, isNumerical: Boolean): Stream[Int] = {
    val source = getColumnAsStream(columnNumber)

    // in case it is numerical we convert the value to double
    val realValue = if (isNumerical) valueToMatch.toDouble else valueToMatch

    // get the indexes of the rows that match the condition
    val indexes = source.getLines().toStream.zipWithIndex.tail.tail.collect {
      case (line, index) if line == realValue.toString => index
    }

    indexes
  }

  /**
   * This method gets the number of columns
   *
   * @return - the number of columns
   */
  def getNumberOfColumns(): Int = {
    Paths.get(path).toFile.listFiles().length
  }
}

object OperationsResultFile {
  val defaultColumn = 0
  val numberOfMetadataLines = 2

  /**
   * This method creates new file
   *
   * @param workingDirectory - the current working directory
   *
   * @return - the new output file
   */
  def createNewFile()(implicit workingDirectory: String): OperationsResultFile = {
    val uuid = UUID.randomUUID()

    val outputPath = workingDirectory + uuid.toString

    Files.createDirectory(Paths.get(outputPath))

    OperationsResultFile(outputPath)
  }
}
