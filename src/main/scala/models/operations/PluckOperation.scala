package models.operations

import models.OperationsResultFile

/**
 * This operation allows us to create file with a given column values
 *
 * @param columnNumber - the number of column that we want to take
 */
case class PluckOperation(columnNumber: Int) extends Operation {
  override def getResult(inputFile: OperationsResultFile)(implicit workingDirectory: String): OperationsResultFile = {
    // create new file
    val outputFile = OperationsResultFile.createNewFile()

    // copy the specific column
    inputFile.copyColumn(columnNumber, outputFile, false)

    outputFile
  }
}
